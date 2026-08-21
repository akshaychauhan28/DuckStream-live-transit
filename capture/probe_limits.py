"""
Probe the SHORT-WINDOW rate limit on the OC Transpo API.

    python capture/probe_limits.py [budget]

Sends up to `budget` requests (default 12) back to back and stops at the first
non-200. Uses VehiclePositions because it's the smaller payload (~38 KB vs
~331 KB), which matters if the limit turns out to be measured in bandwidth
rather than calls.

WHAT THIS CAN AND CANNOT TELL YOU
---------------------------------
It can detect a per-second or per-minute rate limit. Those reset in seconds, so
tripping one is harmless.

It CANNOT safely detect a per-day / per-week / per-month quota, and it does not
try. Discovering a quota by exhausting it means losing collection for the rest
of that period — for a weekly quota, that's up to seven days of archive that
cannot be recovered. If the portal doesn't state your quota, ask OC Transpo
rather than probing for it.

The script aborts immediately if a response looks like a quota rather than a
rate limit, so an accidental discovery costs one call, not the budget.

COST
----
Every call here counts against whatever quota you have. The default of 12 is
deliberately small. If your quota turns out to be ~1,000/week, this run is
about 1% of it.
"""

import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from capture import FEEDS, REQUEST_TIMEOUT_SECONDS  # noqa: E402

URL = FEEDS["vehicle_positions"]

# Azure APIM phrases these two very differently, and the distinction is the
# whole point of the script.
QUOTA_MARKERS = ("out of call volume", "quota", "replenished")
RATE_MARKERS = ("rate limit", "too many requests", "try again in")


def classify(status: int, body: str, headers) -> str:
    low = body.lower()
    if any(m in low for m in QUOTA_MARKERS):
        return "QUOTA"
    if any(m in low for m in RATE_MARKERS):
        return "RATE"
    if status == 429:
        return "RATE"  # 429 without a clear message: assume the safe reading
    return "OTHER"


def main() -> int:
    key = os.environ.get("OC_TRANSPO_PRIMARY_KEY", "").strip()
    if not key:
        print("OC_TRANSPO_PRIMARY_KEY not set.")
        return 1

    budget = int(sys.argv[1]) if len(sys.argv) > 1 else 12

    print(f"Probing short-window rate limit with up to {budget} rapid calls.")
    print(f"Endpoint: {URL}")
    print("Aborts immediately if a response indicates a QUOTA rather than a rate limit.\n")

    session = requests.Session()
    start = time.monotonic()
    ok = 0

    for i in range(1, budget + 1):
        t0 = time.monotonic()
        try:
            r = session.get(
                URL,
                headers={"Ocp-Apim-Subscription-Key": key},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except Exception as exc:
            print(f"  {i:2d}  request failed: {type(exc).__name__}: {exc}")
            break

        elapsed = (time.monotonic() - t0) * 1000
        note = ""
        interesting = {k: v for k, v in r.headers.items()
                       if any(t in k.lower() for t in
                              ("ratelimit", "quota", "retry-after", "throttle"))}
        if interesting:
            note = "  " + " ".join(f"{k}={v}" for k, v in interesting.items())

        print(f"  {i:2d}  status={r.status_code}  {elapsed:6.0f}ms  "
              f"{len(r.content):,} bytes{note}")

        if r.status_code == 200:
            ok += 1
            continue

        kind = classify(r.status_code, r.text, r.headers)
        print(f"\n  --- non-200 response, classified as {kind} ---")
        print("  " + r.text[:600].replace("\n", "\n  "))
        if r.headers.get("Retry-After"):
            print(f"\n  Retry-After: {r.headers['Retry-After']}")

        if kind == "QUOTA":
            print("\n  !! STOPPING. That reads as a long-window quota, not a rate limit.")
            print("     Do not retry. Note the replenishment time above, and set")
            print("     CAPTURE_INTERVAL_SECONDS so daily calls stay well inside it.")
        elif kind == "RATE":
            print(f"\n  Short-window rate limit hit after {ok} successful calls "
                  f"in {time.monotonic() - start:.1f}s.")
            print("     This resets quickly. Keep polling intervals above this rate.")
        break

    duration = time.monotonic() - start
    print(f"\n{'='*60}")
    print(f"  successful calls: {ok}")
    print(f"  elapsed:          {duration:.1f}s")
    if ok:
        print(f"  achieved rate:    {ok/duration:.2f} calls/sec "
              f"({ok/duration*60:.0f}/min)")
    if ok == budget:
        print(f"\n  No rate limit hit within {budget} rapid calls.")
        print("  That rules out an aggressive per-second/per-minute limit.")
        print("  It says NOTHING about a daily or weekly quota — check the portal")
        print("  or ask OC Transpo for that one.")
    print(f"  run at: {datetime.now(timezone.utc).isoformat()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
