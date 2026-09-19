"""
How late are the buses, across the whole archive.

    python query/delay_archive.py
    python query/delay_archive.py --within 60    # stricter arrival proxy

The feed never reports when a bus actually arrived. The closest thing available
is the last prediction made before it got there: seconds out, with the bus in
sight of the stop, that estimate is effectively the arrival time.

The Parquet predictions table keeps one row per change with the window it was
valid for, so "the last prediction for this stop" is just the row with the
largest last_seen. Requiring that it was made shortly before the predicted
arrival keeps only stops a bus was actually approaching.

That is a stricter and more defensible filter than delay_check.py, which works
off a single poll and separates real predictions from timetable echoes by how
far ahead the stop is.
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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--within", type=int, default=120,
                        help="seconds before predicted arrival for the final "
                             "prediction to count as an arrival")
    args = parser.parse_args()

    predictions = str(PARQUET / "predictions" / "dt=*" / "part.parquet").replace("\\", "/")
    trips = str(PARQUET / "trips" / "dt=*" / "part.parquet").replace("\\", "/")

    con = duckdb.connect()

    dates = [r[0] for r in con.execute(
        f"SELECT DISTINCT start_date FROM read_parquet('{predictions}') "
        f"WHERE start_date IS NOT NULL ORDER BY 1"
    ).fetchall()]

    # A service day's scheduled times all count from one instant, so only one
    # conversion per date is needed. Done in Python because that is where the
    # tested "noon minus 12h" logic lives, then handed to SQL as a lookup.
    bundles = {d: bundle_for(d) for d in dates}
    bases = pl.DataFrame({
        "start_date": dates,
        "base_epoch": [scheduled_epoch(d, "00:00:00") for d in dates],
    })
    con.register("bases", bases)

    stop_times = str(ROOT / bundles[dates[0]]["path"] / "stop_times.parquet").replace("\\", "/")
    versions = {b["feed_version"] for b in bundles.values()}
    print(f"days            {len(dates)}  ({dates[0]} to {dates[-1]})")
    print(f"timetable       {', '.join(sorted(versions))}")
    print(f"arrival proxy   final prediction made within {args.within}s of arrival")

    con.execute(f"""
        CREATE TEMP TABLE arrivals AS
        WITH final AS (
            SELECT trip_id, start_date, stop_sequence, arrival_time, last_seen,
                   row_number() OVER (
                       PARTITION BY trip_id, start_date, stop_sequence
                       ORDER BY last_seen DESC
                   ) AS rn
            FROM read_parquet('{predictions}')
            WHERE arrival_time IS NOT NULL AND start_date IS NOT NULL
        ),
        approached AS (
            SELECT trip_id, start_date, stop_sequence, arrival_time
            FROM final
            WHERE rn = 1 AND arrival_time - last_seen BETWEEN 0 AND {args.within}
        ),
        scheduled AS (
            SELECT a.trip_id, a.start_date, a.stop_sequence, a.arrival_time,
                   b.base_epoch + (
                       3600 * CAST(split_part(st.arrival_time, ':', 1) AS BIGINT)
                       + 60 * CAST(split_part(st.arrival_time, ':', 2) AS BIGINT)
                       + CAST(split_part(st.arrival_time, ':', 3) AS BIGINT)
                   ) AS scheduled_epoch
            FROM approached a
            JOIN bases b ON b.start_date = a.start_date
            JOIN read_parquet('{stop_times}') st
              ON st.trip_id = a.trip_id
             AND CAST(st.stop_sequence AS INTEGER) = a.stop_sequence
            WHERE st.arrival_time IS NOT NULL
        )
        SELECT s.*, s.arrival_time - s.scheduled_epoch AS delay,
               t.route_id
        FROM scheduled s
        LEFT JOIN (
            SELECT DISTINCT trip_id, start_date, route_id
            FROM read_parquet('{trips}') WHERE route_id IS NOT NULL
        ) t USING (trip_id, start_date)
        WHERE abs(s.arrival_time - s.scheduled_epoch) <= 7200
    """)

    total = con.execute("SELECT count(*) FROM arrivals").fetchone()[0]
    print(f"arrivals matched {total:,}")
    if not total:
        print("Nothing matched. Try a larger --within.")
        return 1

    print()
    row = con.execute("""
        SELECT median(delay), quantile_cont(delay, 0.10), quantile_cont(delay, 0.25),
               quantile_cont(delay, 0.75), quantile_cont(delay, 0.90),
               100.0 * sum(CASE WHEN abs(delay) <= 60 THEN 1 ELSE 0 END) / count(*),
               100.0 * sum(CASE WHEN delay > 300 THEN 1 ELSE 0 END) / count(*),
               100.0 * sum(CASE WHEN delay < -60 THEN 1 ELSE 0 END) / count(*)
        FROM arrivals
    """).fetchone()
    print(f"  median delay      {row[0]:>7.0f}s")
    print(f"  p10 / p25         {row[1]:>7.0f}s  {row[2]:.0f}s")
    print(f"  p75 / p90         {row[3]:>7.0f}s  {row[4]:.0f}s")
    print(f"  within 60s        {row[5]:>7.1f}%")
    print(f"  over 5 min late   {row[6]:>7.1f}%")
    print(f"  over 1 min early  {row[7]:>7.1f}%")

    print("\n  by hour of day (Ottawa local)")
    for hour, n, med, late in con.execute("""
        SELECT CAST(strftime(to_timestamp(arrival_time) AT TIME ZONE 'America/Toronto',
                             '%H') AS INTEGER) AS hour,
               count(*), median(delay),
               100.0 * sum(CASE WHEN delay > 300 THEN 1 ELSE 0 END) / count(*)
        FROM arrivals GROUP BY 1 HAVING count(*) >= 200 ORDER BY 1
    """).fetchall():
        bar = "#" * int(max(med, 0) / 20)
        print(f"    {hour:02d}:00  {n:>7,}  {med:>6.0f}s  {late:>5.1f}% late  {bar}")

    print("\n  worst routes (500+ arrivals)")
    for route, n, med, late in con.execute("""
        SELECT route_id, count(*), median(delay),
               100.0 * sum(CASE WHEN delay > 300 THEN 1 ELSE 0 END) / count(*)
        FROM arrivals WHERE route_id IS NOT NULL
        GROUP BY 1 HAVING count(*) >= 500 ORDER BY median(delay) DESC LIMIT 8
    """).fetchall():
        print(f"    route {route:>4}  {n:>7,} arrivals  {med:>6.0f}s median  {late:>5.1f}% over 5 min")

    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
