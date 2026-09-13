"""
Summarise what's actually in the raw capture archive.

    python capture/inspect_archive.py [raw_dir]

Answers "is capture working?" without touching the API. Reads the gzip files on
disk, counts frames per feed, reports failures, and shows the time span covered
so gaps are visible at a glance.

Safe to run while capture.py is still writing — read_frames tolerates a
partially written final frame.
"""

import os
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from frames import read_frames  # noqa: E402


def main() -> int:
    raw = Path(sys.argv[1] if len(sys.argv) > 1
               else os.environ.get("CAPTURE_OUTPUT_DIR", "./raw"))
    if not raw.exists():
        print(f"No archive at {raw.resolve()} — has capture.py run yet?")
        return 1

    # *.gz covers both the current `.frames.gz` files and the `.jsonl.gz` files
    # written by the first version of capture.py.
    files = sorted(raw.glob("capture_*.gz"))
    if not files:
        print(f"{raw.resolve()} exists but holds no capture files yet.")
        return 1

    per_feed = Counter()
    failures = Counter()
    payload_bytes = Counter()
    times = []
    feed_times = defaultdict(list)
    errors = []
    unfinished = []
    damaged = []

    for path in files:
        stats = {}
        for meta, body in read_frames(path, stats):
            feed = meta.get("feed", "?")
            per_feed[feed] += 1
            payload_bytes[feed] += len(body)
            if meta.get("status") != 200:
                failures[feed] += 1
                if len(errors) < 5:
                    errors.append(
                        f"{meta.get('fetched_at')} {feed} "
                        f"status={meta.get('status')} "
                        f"{meta.get('error') or meta.get('error_body', '')[:120]}"
                    )
            if meta.get("fetched_at"):
                times.append(meta["fetched_at"])
                feed_times[feed].append(datetime.fromisoformat(meta["fetched_at"]))

        if stats.get("unreadable_bytes"):
            damaged.append((path.name, stats["unreadable_bytes"]))
        elif stats.get("incomplete_tail"):
            unfinished.append(path.name)

    disk = sum(p.stat().st_size for p in files)
    total = sum(per_feed.values())

    print(f"archive:   {raw.resolve()}")
    print(f"files:     {len(files)}  ({disk/1e6:,.1f} MB on disk)")
    print(f"frames:    {total:,}")

    if times:
        times.sort()
        first = datetime.fromisoformat(times[0])
        last = datetime.fromisoformat(times[-1])
        span = (last - first).total_seconds()
        print(f"span:      {first.isoformat()}")
        print(f"           {last.isoformat()}")
        print(f"           {span/3600:.2f} hours")

    print()
    for feed in sorted(per_feed):
        n = per_feed[feed]
        bad = failures[feed]
        mb = payload_bytes[feed] / 1e6
        line = f"  {feed:20s} {n:6,} polls   {mb:8,.1f} MB payload"
        if bad:
            line += f"   {bad} FAILED ({100*bad/n:.1f}%)"
        print(line)
        stamps = sorted(feed_times[feed])
        if len(stamps) > 1:
            # Median of the gaps between consecutive polls, rather than span /
            # count: a restart fires every feed immediately, which drags a
            # simple average below the configured interval.
            gaps = [(b - a).total_seconds() for a, b in zip(stamps, stamps[1:])]
            print(f"  {'':20s} interval ~{statistics.median(gaps):.0f}s, "
                  f"longest gap {max(gaps):.0f}s")

    if unfinished:
        print("\n  still being written, or cut off mid-write (normal while capture runs):")
        for name in unfinished:
            print(f"    {name}")

    if damaged:
        print("\n  DAMAGED — everything after the damage point could not be read:")
        for name, n in damaged:
            print(f"    {name}: {n:,} bytes unreadable")

    if errors:
        print("\n  recent failures:")
        for e in errors:
            print(f"    {e}")

    if times and len(times) > 2:
        # Rough projection using observed bytes-on-disk per hour.
        span_h = (datetime.fromisoformat(times[-1])
                  - datetime.fromisoformat(times[0])).total_seconds() / 3600
        if span_h > 0.05:
            per_day = disk / span_h * 24
            print(f"\n  at this rate: {per_day/1e6:,.0f} MB/day "
                  f"-> {per_day*30/1e9:,.1f} GB over 30 days")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
