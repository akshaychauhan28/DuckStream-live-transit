"""
How much of the TripUpdates stream is repetition, and what is safe to key on?

    python scripts/measure_prediction_churn.py [--hours 2]

VehiclePositions turned out to be 57.5% duplicates, deduped safely on
(vehicle_id, vehicle_timestamp). TripUpdates is a different shape and that
result does not transfer, so it gets measured rather than assumed.

Every poll re-sends every prediction for every active trip, so the same stop
appears again and again. But unlike a vehicle position, a prediction can be
*revised* — the bus falls behind and the estimate moves. Those revisions are
real information, and the question is which of them to keep.
"""

import argparse
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
for sub in ("capture", "ingestion"):
    sys.path.insert(0, str(ROOT / sub))

from decode import decode_trip_updates  # noqa: E402
from frames import read_frames  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--hours", type=int, default=2, help="how many hourly files")
    args = parser.parse_args()

    files = sorted((ROOT / "raw").glob("capture_*.gz"))[-args.hours:]
    if not files:
        print("no capture files in raw/")
        return 1

    rows = 0
    polls = 0
    identical = set()                       # trip, date, stop, predicted time
    stops = defaultdict(list)               # trip, date, stop -> predicted times
    trip_stamps = defaultdict(set)          # trip, date -> trip_update_timestamps
    trip_polls = Counter()

    for path in files:
        for meta, body in read_frames(path):
            if meta.get("feed") != "trip_updates" or meta.get("status") != 200:
                continue
            trips, predictions = decode_trip_updates(meta, body)
            polls += 1
            for trip in trips:
                key = (trip["trip_id"], trip["start_date"])
                trip_polls[key] += 1
                if trip["trip_update_timestamp"] is not None:
                    trip_stamps[key].add(trip["trip_update_timestamp"])
            for p in predictions:
                if p["arrival_time"] is None:
                    continue
                rows += 1
                stop_key = (p["trip_id"], p["start_date"], p["stop_sequence"])
                identical.add(stop_key + (p["arrival_time"],))
                stops[stop_key].append(p["arrival_time"])

    print(f"files            {len(files)}")
    print(f"TripUpdates polls {polls}")
    print(f"prediction rows  {rows:,}")
    print()
    print(f"unique (trip, stop, predicted time)  {len(identical):,}  "
          f"— keeping every revision")
    print(f"unique (trip, stop)                  {len(stops):,}  "
          f"— keeping one row per stop")
    print(f"identical repeats discarded          {rows - len(identical):,}  "
          f"({100 * (rows - len(identical)) / max(rows, 1):.1f}%)")

    # How often does a prediction actually move?
    revisions = [len(set(v)) for v in stops.values()]
    never = sum(1 for r in revisions if r == 1)
    print()
    print("revisions per stop (distinct predicted times seen)")
    print(f"  never revised   {never:,} of {len(revisions):,} "
          f"({100 * never / max(len(revisions), 1):.1f}%)")
    print(f"  median          {statistics.median(revisions):.0f}")
    print(f"  p90             {sorted(revisions)[int(len(revisions) * 0.9)]}")
    print(f"  max             {max(revisions)}")

    # How far do predictions move when they do move?
    swings = []
    for values in stops.values():
        distinct = list(dict.fromkeys(values))
        if len(distinct) > 1:
            swings.append(max(distinct) - min(distinct))
    if swings:
        swings.sort()
        print()
        print(f"when a prediction moves, by how much (seconds)")
        print(f"  median {statistics.median(swings):.0f}   "
              f"p90 {swings[int(len(swings) * 0.9)]}   max {max(swings)}")

    # A trip whose own timestamp has not changed cannot have new predictions.
    stale = sum(1 for key, stamps in trip_stamps.items()
                if len(stamps) < trip_polls[key])
    print()
    print(f"trips seen in more than one poll whose trip_update_timestamp")
    print(f"repeated at least once: {stale:,} of {len(trip_stamps):,} "
          f"({100 * stale / max(len(trip_stamps), 1):.1f}%)")
    print("Those polls carried nothing new for that trip and could be skipped")
    print("before touching any of their predictions.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
