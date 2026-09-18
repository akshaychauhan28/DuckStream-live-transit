"""
How late are the buses, and how much of that is actually measured?

    python query/delay_check.py                 # newest poll in the archive
    python query/delay_check.py --polls 10      # average over the last 10 polls

Delay is predicted arrival minus scheduled arrival, so this joins captured
TripUpdates predictions to the timetable that was in force on the service date.

The headline number on its own is misleading, which is why this always prints
the breakdown by horizon as well. For stops far ahead OC Transpo returns the
scheduled time unchanged, because it has nothing better to say yet. Those rows
are the timetable echoed back rather than an observation, and counting them
drags the median toward zero and makes the service look far more punctual than
it is.
"""

import argparse
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
for sub in ("capture", "ingestion", "processing"):
    sys.path.insert(0, str(ROOT / sub))

import duckdb  # noqa: E402
import polars as pl  # noqa: E402

from decode import decode_trip_updates  # noqa: E402
from frames import read_frames  # noqa: E402
from gtfs_time import scheduled_epoch  # noqa: E402
from static_gtfs import bundle_for  # noqa: E402

# Anything beyond two hours is a data problem rather than a late bus.
SANE_LIMIT = 7200

HORIZONS = [
    ("already at the stop", -10**9, 0),
    ("next 5 min", 0, 300),
    ("5 to 15 min", 300, 900),
    ("15 to 30 min", 900, 1800),
    ("30 to 60 min", 1800, 3600),
    ("over 60 min", 3600, 10**9),
]


def horizon_of(seconds: int) -> str:
    for name, low, high in HORIZONS:
        if low <= seconds < high:
            return name
    return HORIZONS[-1][0]


def latest_frames(polls: int):
    """The most recent successful TripUpdates frames, newest file first."""
    files = sorted((ROOT / "raw").glob("capture_*.gz"), reverse=True)
    found = []
    for path in files:
        frames = [
            (meta, body) for meta, body in read_frames(path)
            if meta.get("feed") == "trip_updates" and meta.get("status") == 200
        ]
        found = frames[-(polls - len(found)):] + found if found else frames[-polls:]
        if len(found) >= polls:
            break
    return found


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--polls", type=int, default=1, help="how many polls to use")
    args = parser.parse_args()

    frames = latest_frames(args.polls)
    if not frames:
        print("No TripUpdates frames found in raw/")
        return 1

    predictions, route_of, fetched = [], {}, {}
    for meta, body in frames:
        trips, preds = decode_trip_updates(meta, body)
        for trip in trips:
            route_of[(trip["trip_id"], trip["start_date"])] = trip["route_id"]
        for p in preds:
            if p["arrival_time"] is not None:
                predictions.append(p)
                fetched[id(p)] = p["fetched_at"]
    if not predictions:
        print("No arrival predictions in those polls")
        return 1

    service_date = max(p["start_date"] for p in predictions if p["start_date"])
    bundle = bundle_for(service_date)
    stop_times = str(ROOT / bundle["path"] / "stop_times.parquet").replace("\\", "/")

    first = datetime.fromtimestamp(min(p["fetched_at"] for p in predictions), timezone.utc)
    last = datetime.fromtimestamp(max(p["fetched_at"] for p in predictions), timezone.utc)

    print(f"polls          {len(frames)}")
    print(f"window         {first:%Y-%m-%d %H:%M} to {last:%H:%M} UTC")
    print(f"timetable      {bundle['feed_version']} "
          f"({bundle['feed_start_date']} to {bundle['feed_end_date']})")
    print(f"predictions    {len(predictions):,}")

    # Join to the timetable. DuckDB reads the Parquet directly; every column in
    # it is text, so stop_sequence needs casting to match.
    frame = pl.DataFrame([
        {"row": i, "trip_id": p["trip_id"], "start_date": p["start_date"],
         "stop_sequence": p["stop_sequence"], "predicted": p["arrival_time"],
         "fetched_at": p["fetched_at"]}
        for i, p in enumerate(predictions)
    ])
    con = duckdb.connect()
    con.register("pred", frame)
    joined = con.execute(f"""
        SELECT p.row, p.trip_id, p.start_date, p.predicted, p.fetched_at,
               st.arrival_time
        FROM pred p
        JOIN read_parquet('{stop_times}') st
          ON st.trip_id = p.trip_id
         AND CAST(st.stop_sequence AS INTEGER) = p.stop_sequence
    """).fetchall()
    con.close()

    matched = len(joined)
    print(f"matched        {matched:,} ({100 * matched / len(predictions):.1f}%)")

    # Same scheduled time repeats across polls, so cache the conversion.
    cache = {}
    rows = []
    for _, trip_id, start_date, predicted, fetched_at, scheduled_text in joined:
        key = (start_date, scheduled_text)
        if key not in cache:
            cache[key] = scheduled_epoch(start_date, scheduled_text)
        delay = predicted - cache[key]
        if abs(delay) > SANE_LIMIT:
            continue
        rows.append((delay, predicted - fetched_at, route_of.get((trip_id, start_date))))

    buckets = defaultdict(list)
    zeros = defaultdict(int)
    for delay, horizon, _ in rows:
        name = horizon_of(horizon)
        buckets[name].append(delay)
        if delay == 0:
            zeros[name] += 1

    print()
    print("  How far ahead        rows   exactly on time   median delay")
    print("  " + "-" * 58)
    for name, _, _ in HORIZONS:
        values = buckets.get(name)
        if not values:
            continue
        share = 100 * zeros[name] / len(values)
        print(f"  {name:<18} {len(values):>7,}   {share:>13.1f}%   {statistics.median(values):>9.0f}s")

    near = [d for name in ("already at the stop", "next 5 min") for d in buckets.get(name, [])]
    every = [d for values in buckets.values() for d in values]

    def summary(values):
        values = sorted(values)
        return (
            statistics.median(values),
            100 * sum(1 for v in values if abs(v) <= 60) / len(values),
            100 * sum(1 for v in values if v > 300) / len(values),
        )

    all_median, all_ontime, all_late = summary(every)
    near_median, near_ontime, near_late = summary(near)

    print()
    print("                          all predictions   within 5 min of stop")
    print("  " + "-" * 58)
    print(f"  median delay       {all_median:>16.0f}s {near_median:>19.0f}s")
    print(f"  within 60s         {all_ontime:>16.1f}% {near_ontime:>19.1f}%")
    print(f"  over 5 min late    {all_late:>16.1f}% {near_late:>19.1f}%")
    print()
    print(f"  {100 * sum(zeros.values()) / len(every):.1f}% of all rows are the timetable echoed back,")
    print("  not a measurement. The right-hand column excludes them.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
