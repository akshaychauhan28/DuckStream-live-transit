"""
Build the database the public demo serves.

    python api/build_demo_db.py
    python api/build_demo_db.py --days 7     # keep only the most recent week

WHY A SEPARATE DATABASE, AND WHY ONE TABLE

The guardrail stops malicious SQL. It cannot stop a language model writing SQL
that is wrong.

Pointed at the raw predictions table, a model asked "how punctual is route 95?"
would average every row — including the ~31% that are the timetable echoed back
for stops the bus is nowhere near — and answer that Ottawa buses run exactly on
time. That is the mistake this project already made once, and the model has no
way to know about it.

So the demo serves one table with the analysis already done: one row per
arrival, delay computed the defensible way, route and stop names resolved. The
model writes simple aggregations over clean data instead of reconstructing the
reasoning from parts. A small schema also fits comfortably in a prompt.

The schema is part of the guardrail, not separate from it.

WHAT AN ARRIVAL MEANS HERE

The feed never reports when a bus actually arrived. The closest available proxy
is the last prediction made before it got there, seconds out, with the bus in
sight of the stop. See query/delay_archive.py for the same logic.

Rows where the bus was never observed approaching are absent, so this measures
arrivals that happened rather than overall service reliability. That caveat is
stored in the `about` table so the model can pass it on.

The file is written with external access disabled in mind: it holds real
tables, not views over Parquet, so the serving connection never needs to touch
the filesystem.
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
for sub in ("ingestion", "processing"):
    sys.path.insert(0, str(ROOT / sub))

import duckdb  # noqa: E402
import polars as pl  # noqa: E402

from gtfs_time import scheduled_epoch  # noqa: E402
from static_gtfs import bundle_for  # noqa: E402

PARQUET = ROOT / "data" / "parquet"
OUT = ROOT / "data" / "demo.duckdb"

ARRIVAL_WINDOW = 120   # seconds before predicted arrival to count as arrived
SANE_DELAY = 7200      # beyond two hours is a data problem, not a late bus


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--days", type=int, help="keep only the most recent N days")
    parser.add_argument("--out", default=str(OUT), help="output database path")
    args = parser.parse_args()

    predictions = str(PARQUET / "predictions" / "dt=*" / "part.parquet").replace("\\", "/")
    trips = str(PARQUET / "trips" / "dt=*" / "part.parquet").replace("\\", "/")

    scratch = duckdb.connect()
    dates = [r[0] for r in scratch.execute(
        f"SELECT DISTINCT start_date FROM read_parquet('{predictions}') "
        f"WHERE start_date IS NOT NULL ORDER BY 1"
    ).fetchall()]
    if args.days:
        dates = dates[-args.days:]
    if not dates:
        print("No prediction data found. Run storage/writer.py first.")
        return 1

    bundles = {d: bundle_for(d) for d in dates}
    bundle_path = ROOT / bundles[dates[-1]]["path"]
    stop_times = str(bundle_path / "stop_times.parquet").replace("\\", "/")
    stops = str(bundle_path / "stops.parquet").replace("\\", "/")
    routes = str(bundle_path / "routes.parquet").replace("\\", "/")

    # One conversion per service date, using the tested noon-minus-12h rule,
    # handed to SQL as a lookup.
    bases = pl.DataFrame({
        "start_date": dates,
        "base_epoch": [scheduled_epoch(d, "00:00:00") for d in dates],
    })
    scratch.close()

    out_path = Path(args.out)
    if out_path.exists():
        out_path.unlink()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(out_path))
    con.register("bases", bases)
    date_list = ", ".join(f"'{d}'" for d in dates)

    print(f"days           {len(dates)}  ({dates[0]} to {dates[-1]})")
    print(f"timetable      {sorted({b['feed_version'] for b in bundles.values()})[0]}")
    print("building arrivals ...")

    con.execute(f"""
        CREATE TABLE arrivals AS
        WITH final AS (
            SELECT trip_id, start_date, stop_sequence, arrival_time, last_seen,
                   row_number() OVER (
                       PARTITION BY trip_id, start_date, stop_sequence
                       ORDER BY last_seen DESC
                   ) AS rn
            FROM read_parquet('{predictions}')
            WHERE arrival_time IS NOT NULL
              AND start_date IN ({date_list})
        ),
        approached AS (
            SELECT trip_id, start_date, stop_sequence, arrival_time
            FROM final
            WHERE rn = 1 AND arrival_time - last_seen BETWEEN 0 AND {ARRIVAL_WINDOW}
        ),
        with_schedule AS (
            SELECT a.trip_id, a.start_date, a.stop_sequence, a.arrival_time,
                   b.base_epoch + (
                       3600 * CAST(split_part(st.arrival_time, ':', 1) AS BIGINT)
                       + 60 * CAST(split_part(st.arrival_time, ':', 2) AS BIGINT)
                       + CAST(split_part(st.arrival_time, ':', 3) AS BIGINT)
                   ) AS scheduled_epoch,
                   st.stop_id
            FROM approached a
            JOIN bases b ON b.start_date = a.start_date
            JOIN read_parquet('{stop_times}') st
              ON st.trip_id = a.trip_id
             AND CAST(st.stop_sequence AS INTEGER) = a.stop_sequence
            WHERE st.arrival_time IS NOT NULL
        ),
        named AS (
            SELECT w.*, t.route_id, s.stop_name, r.route_short_name, r.route_long_name
            FROM with_schedule w
            LEFT JOIN (
                SELECT DISTINCT trip_id, start_date, route_id
                FROM read_parquet('{trips}') WHERE route_id IS NOT NULL
            ) t USING (trip_id, start_date)
            LEFT JOIN read_parquet('{stops}') s ON s.stop_id = w.stop_id
            LEFT JOIN read_parquet('{routes}') r ON r.route_id = t.route_id
        )
        SELECT
            route_id,
            coalesce(route_short_name, route_id)                AS route_name,
            route_long_name                                     AS route_description,
            stop_id,
            stop_name,
            trip_id,
            start_date                                          AS service_date,
            to_timestamp(scheduled_epoch) AT TIME ZONE 'America/Toronto'
                                                                AS scheduled_at,
            to_timestamp(arrival_time) AT TIME ZONE 'America/Toronto'
                                                                AS arrived_at,
            arrival_time - scheduled_epoch                      AS delay_seconds,
            CAST(strftime(to_timestamp(arrival_time)
                 AT TIME ZONE 'America/Toronto', '%H') AS INTEGER) AS hour_local,
            strftime(to_timestamp(arrival_time)
                 AT TIME ZONE 'America/Toronto', '%A')           AS day_of_week
        FROM named
        WHERE abs(arrival_time - scheduled_epoch) <= {SANE_DELAY}
    """)

    rows = con.execute("SELECT count(*) FROM arrivals").fetchone()[0]
    print(f"arrivals       {rows:,}")

    # A short description the model can be shown, so the caveats travel with
    # the data rather than living only in a README.
    con.execute("""
        CREATE TABLE about (topic VARCHAR, detail VARCHAR)
    """)
    con.executemany("INSERT INTO about VALUES (?, ?)", [
        ("what this is",
         "Real arrivals of OC Transpo buses in Ottawa, collected by polling the "
         "live GTFS-Realtime feed every 45 seconds and joined to the published "
         "timetable."),
        ("delay_seconds",
         "Actual arrival minus scheduled arrival. Positive means late."),
        ("how arrival is known",
         "The feed never reports actual arrivals. Each row uses the last "
         "prediction made within 120 seconds of the bus reaching the stop, "
         "which is the closest available proxy."),
        ("what is missing",
         "Only stops a bus was observed approaching appear. Cancelled trips and "
         "buses that vanished from the feed are absent, so this describes "
         "arrivals that happened rather than overall reliability."),
        ("timezone", "scheduled_at and arrived_at are Ottawa local time."),
    ])

    summary = con.execute("""
        SELECT count(*), count(DISTINCT route_id), count(DISTINCT stop_id),
               min(service_date), max(service_date),
               median(delay_seconds),
               100.0 * sum(CASE WHEN abs(delay_seconds) <= 60 THEN 1 ELSE 0 END) / count(*)
        FROM arrivals
    """).fetchone()
    con.close()

    size = out_path.stat().st_size
    print(f"routes         {summary[1]}")
    print(f"stops          {summary[2]:,}")
    print(f"service dates  {summary[3]} to {summary[4]}")
    print(f"median delay   {summary[5]:.0f}s")
    print(f"within 60s     {summary[6]:.1f}%")
    print(f"\nwrote {out_path.relative_to(ROOT)}  ({size/1e6:,.1f} MB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
