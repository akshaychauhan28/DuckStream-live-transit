"""
Which timetable version actually matches each day of collected data?

    python scripts/check_bundle_match.py

A GTFS bundle declares the dates it covers in feed_info.txt. That declaration
is not evidence it matches the data: trip_id is an internal identifier, and an
agency regenerates them when it republishes the schedule.

On 2026-09-28 the bundle held since Sep 17 still declared coverage through
2026-10-10 while matching 31% of the trip_ids being collected. The join
silently returned almost nothing, and nothing in the metadata said so.

So the version in force is decided by measuring the overlap between the
trip_ids in the data and the trip_ids in each bundle, not by reading dates.
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

import duckdb  # noqa: E402

GOOD = 90.0   # per cent of trip_ids that must match to call a bundle usable
PREDICTIONS = str(ROOT / "data" / "parquet" / "predictions" / "dt=*" /
                  "part.parquet").replace("\\", "/")


def main() -> int:
    manifest_path = ROOT / "data" / "gtfs" / "manifest.json"
    if not manifest_path.exists():
        print("No GTFS bundles held. Run ingestion/static_gtfs.py first.")
        return 1
    versions = json.loads(manifest_path.read_text())["versions"]

    con = duckdb.connect()
    con.execute("SET enable_progress_bar=false")
    dates = [r[0] for r in con.execute(
        f"SELECT DISTINCT start_date FROM read_parquet('{PREDICTIONS}') "
        f"WHERE start_date IS NOT NULL ORDER BY 1").fetchall()]
    if not dates:
        print("No collected data found. Run storage/writer.py first.")
        return 1

    print("Share of collected trip_ids found in each timetable version")
    print()
    print("  " + "service day".ljust(14)
          + "".join(v["feed_version"].rjust(12) for v in versions)
          + "   in force")
    print("  " + "-" * (14 + 12 * len(versions) + 12))

    unmatched = []
    for day in dates:
        rates = []
        for version in versions:
            stop_times = str(ROOT / version["path"] /
                             "stop_times.parquet").replace("\\", "/")
            pct = con.execute(f"""
                SELECT 100.0 * count(DISTINCT CASE WHEN s.trip_id IS NOT NULL
                                                   THEN t.trip_id END)
                       / nullif(count(DISTINCT t.trip_id), 0)
                FROM (SELECT DISTINCT trip_id FROM read_parquet('{PREDICTIONS}')
                      WHERE start_date = '{day}') t
                LEFT JOIN (SELECT DISTINCT trip_id
                           FROM read_parquet('{stop_times}')) s USING (trip_id)
            """).fetchone()[0] or 0.0
            rates.append(pct)

        best = max(rates)
        winner = versions[rates.index(best)]["feed_version"] if best >= GOOD else "NONE"
        pretty = f"{day[:4]}-{day[4:6]}-{day[6:]}"
        if best < GOOD:
            unmatched.append(pretty)
        print("  " + pretty.ljust(14)
              + "".join(f"{r:>11.1f}%" for r in rates)
              + f"   {winner}")
    con.close()

    print()
    print(f"  {len(dates) - len(unmatched)} of {len(dates)} days match a held "
          f"timetable at {GOOD:.0f}% or better")
    if unmatched:
        print(f"  {len(unmatched)} days match nothing: "
              f"{unmatched[0]} to {unmatched[-1]}")
        print("  Delay cannot be computed for those days until the timetable")
        print("  version that was in force is recovered.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
