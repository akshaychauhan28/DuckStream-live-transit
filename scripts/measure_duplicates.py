"""
How much of the archive is the same bus reporting nothing new?

    python scripts/measure_duplicates.py [raw_dir]

We poll VehiclePositions every 30 seconds, but a bus only updates its own
timestamp when it actually reports. Poll faster than the buses report and the
same reading comes back again — identical position, identical timestamp, new
fetched_at.

This measures three things that decide how the consumer should work:

  1. What share of decoded records are repeats.
  2. How often a vehicle actually reports, which is the real ceiling on useful
     polling frequency.
  3. Whether (vehicle_id, vehicle_timestamp) is safe to dedupe on — that is,
     whether a repeated timestamp ever carries a different position. If it
     does, the key is wrong and dedupe would silently discard real movement.
"""

import statistics
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "capture"))
sys.path.insert(0, str(ROOT / "ingestion"))

from decode import decode_vehicle_positions  # noqa: E402
from frames import read_frames  # noqa: E402


def main() -> int:
    raw = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "raw"
    files = sorted(raw.glob("capture_*.gz"))
    if not files:
        print(f"no capture files in {raw}")
        return 1

    total = 0
    polls = 0
    first_seen = {}                      # (vehicle_id, timestamp) -> (lat, lon)
    conflicts = 0                        # same key, different position
    timestamps = defaultdict(set)        # vehicle_id -> distinct timestamps

    for path in files:
        for meta, body in read_frames(path):
            if meta.get("feed") != "vehicle_positions":
                continue
            records = decode_vehicle_positions(meta, body)
            if not records:
                continue
            polls += 1
            for record in records:
                total += 1
                key = (record["vehicle_id"], record["vehicle_timestamp"])
                position = (record["latitude"], record["longitude"])
                if key in first_seen:
                    if first_seen[key] != position:
                        conflicts += 1
                else:
                    first_seen[key] = position
                    timestamps[record["vehicle_id"]].add(record["vehicle_timestamp"])

    unique = len(first_seen)
    duplicates = total - unique

    print(f"files scanned        {len(files)}")
    print(f"vehicle polls        {polls:,}")
    print(f"records decoded      {total:,}")
    print(f"unique readings      {unique:,}")
    print(f"duplicates           {duplicates:,}  ({100 * duplicates / max(total, 1):.1f}%)")
    print(f"vehicles seen        {len(timestamps):,}")

    # How often does a vehicle actually report? Gaps between its own distinct
    # timestamps. Anything huge is the vehicle going out of service and coming
    # back, so the median is the number that matters.
    gaps = []
    for stamps in timestamps.values():
        ordered = sorted(stamps)
        gaps.extend(b - a for a, b in zip(ordered, ordered[1:]) if 0 < b - a < 3600)

    if gaps:
        gaps.sort()
        pick = lambda p: gaps[int(len(gaps) * p)]
        print()
        print("vehicle report interval (seconds between a bus's own updates)")
        print(f"  min {gaps[0]}   p25 {pick(.25)}   median {statistics.median(gaps):.0f}"
              f"   p75 {pick(.75)}   p90 {pick(.90)}   max {gaps[-1]}")
        under_30 = sum(1 for g in gaps if g <= 30)
        print(f"  {100 * under_30 / len(gaps):.1f}% of updates arrive within 30s")

    print()
    if conflicts:
        print(f"WARNING: {conflicts:,} records shared a (vehicle_id, timestamp) key but "
              f"reported a different position.")
        print("         That key is NOT safe to dedupe on.")
    else:
        print("(vehicle_id, vehicle_timestamp) is safe to dedupe on:")
        print("  every repeated key carried an identical position.")

    if total:
        print()
        print(f"Deduping would keep {100 * unique / total:.1f}% of rows "
              f"({unique:,} of {total:,}).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
