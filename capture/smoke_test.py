"""
One-shot smoke test: does the key work, what comes back, and how big is it?

Run this BEFORE starting continuous capture. It makes a single call to each
feed, reports status and payload size, decodes the protobuf just far enough to
count entities, and writes the responses to tests/fixtures/ so the rest of the
project has real data to develop against.

    python capture/smoke_test.py

Deliberately one call per feed — if your portal quota is tight, this costs you
two calls, not 5,760.
"""

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from capture import FEEDS, REQUEST_TIMEOUT_SECONDS  # noqa: E402

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "tests" / "fixtures"


def main() -> int:
    key = os.environ.get("OC_TRANSPO_PRIMARY_KEY", "").strip()
    if not key:
        print("OC_TRANSPO_PRIMARY_KEY not set. Set it and re-run.")
        return 1

    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    failures = 0

    for name, url in FEEDS.items():
        print(f"\n=== {name} ===")
        print(f"GET {url}")
        try:
            r = requests.get(
                url,
                headers={"Ocp-Apim-Subscription-Key": key},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except Exception as exc:
            print(f"  request failed: {type(exc).__name__}: {exc}")
            failures += 1
            continue

        print(f"  status:        {r.status_code}")
        print(f"  elapsed:       {int(r.elapsed.total_seconds() * 1000)} ms")
        print(f"  content-type:  {r.headers.get('Content-Type')}")
        print(f"  bytes:         {len(r.content):,}")

        # Azure APIM commonly returns quota/rate headers. If present these are
        # the real answer to "what is my limit", better than any documentation.
        quota_headers = {k: v for k, v in r.headers.items()
                         if any(t in k.lower() for t in
                                ("ratelimit", "quota", "retry-after", "throttle"))}
        if quota_headers:
            print("  QUOTA HEADERS (this is your real rate limit):")
            for k, v in quota_headers.items():
                print(f"    {k}: {v}")
        else:
            print("  no rate-limit headers returned")

        if r.status_code != 200:
            print(f"  body: {r.text[:500]}")
            failures += 1
            continue

        out = FIXTURE_DIR / f"{name}_{stamp}.pb"
        out.write_bytes(r.content)
        print(f"  saved fixture: {out.relative_to(FIXTURE_DIR.parent.parent)}")

        # Decode just enough to confirm the bytes are what we think they are.
        try:
            from google.transit import gtfs_realtime_pb2

            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(r.content)
            print(f"  entities:      {len(feed.entity):,}")
            print(f"  feed version:  {feed.header.gtfs_realtime_version}")
            if feed.header.timestamp:
                ts = datetime.fromtimestamp(feed.header.timestamp, timezone.utc)
                print(f"  feed time:     {ts.isoformat()}")
            if feed.entity:
                print("\n  --- first entity (verbatim) ---")
                print("  " + str(feed.entity[0]).replace("\n", "\n  ").rstrip())
        except ImportError:
            print("  (install gtfs-realtime-bindings to decode)")
        except Exception as exc:
            print(f"  decode failed: {type(exc).__name__}: {exc}")
            failures += 1

    print("\nFixtures written to tests/fixtures/ — these are real payloads, so")
    print("check them before committing and keep them out of git if unsure.")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
