"""
DuckStream raw capture — the one component that must not fail.

Fetches the OC Transpo GTFS-Realtime feeds on a fixed interval and appends the
raw, undecoded response bytes to hourly-rotated gzip files. That is all it does.

It deliberately does NOT decode protobuf, validate, transform, dedupe, or
publish to a broker. Every one of those is something that can throw, and this
process runs unattended on a small VM collecting data that can never be
re-fetched. GTFS-RT is a *snapshot* feed — it reports where buses are right
now, and there is no endpoint that returns last Tuesday. Miss 05:00-09:00 and
that rush hour is permanently gone.

So the design rule here is: fewer moving parts than anything else in the repo.
Everything downstream (decode -> Redpanda -> Polars -> Parquet) replays from
the files this writes, which means a bug in the transform layer costs a replay,
not data.

See docs/DECISIONS.md #2 and #3.

Run:
    python capture/capture.py

Stops cleanly on SIGTERM/SIGINT so systemd restarts don't truncate a frame.
"""

import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from frames import append_frame  # noqa: E402

# Load .env when running locally, if python-dotenv happens to be installed.
# On the VM it deliberately isn't — systemd supplies the environment from
# /etc/duckstream/capture.env instead. So this is a local convenience, not a
# dependency, and the VM install stays at exactly one package: requests.
try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except ImportError:
    pass

# --- Feeds -----------------------------------------------------------------
# Confirmed against the OC Transpo developer portal (Azure API Management).
# Auth is a subscription key in the Ocp-Apim-Subscription-Key header.
# Both default to protobuf; we store the bytes exactly as returned.
FEEDS = {
    "vehicle_positions": "https://nextrip-public-api.azure-api.net/octranspo/gtfs-rt-vp/beta/v1/VehiclePositions",
    "trip_updates": "https://nextrip-public-api.azure-api.net/octranspo/gtfs-rt-tp/beta/v1/TripUpdates",
}

REQUEST_TIMEOUT_SECONDS = 20

# Identify ourselves on every request.
#
# OC Transpo publishes no hard quota; instead they ask developers to cache and
# avoid high-frequency polling, and reserve the right to throttle or suspend
# keys that cause excessive load. A descriptive User-Agent means that if anyone
# ever looks at their logs wondering who is pulling this volume, they find a
# named project with a contact point rather than an anonymous scraper. Cheap
# insurance against being switched off without warning.
USER_AGENT = (
    "DuckStream/0.1 "
    "(+https://github.com/akshaychauhan28/DuckStream-live-transit)"
)

# Per-feed poll intervals, in seconds.
#
# Measured 2026-08-20 on real payloads: TripUpdates is ~105KB gzipped per poll
# against VehiclePositions' ~12.5KB, so TripUpdates is ~89% of the archive by
# size. Its signal (predicted arrival times) does not change meaningfully in
# 30s, whereas VehiclePositions at 30s is what gives trajectory resolution for
# speed derivation. So TripUpdates is the natural place to trade resolution for
# disk if you need to.
#
#   both at 30s            -> ~341 MB/day  -> ~10.2 GB over 30 days
#   VP 30s + TU 120s       -> ~112 MB/day  ->  ~3.4 GB over 30 days
#
# CAPTURE_INTERVAL_SECONDS sets the default for every feed; the per-feed
# variables override it.
DEFAULT_INTERVAL = 30
INTERVAL_ENV = {
    "vehicle_positions": "CAPTURE_INTERVAL_VEHICLE_POSITIONS_SECONDS",
    "trip_updates": "CAPTURE_INTERVAL_TRIP_UPDATES_SECONDS",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("capture")

_shutdown = False


def _handle_signal(signum, _frame):
    """Ask the main loop to finish the current tick and exit cleanly."""
    global _shutdown
    log.info("signal %s received, shutting down after this tick", signum)
    _shutdown = True


class HourlyWriter:
    """
    Appends frames to one file per UTC hour, with a fresh file for each run.

    Two rules keep every file readable at every moment, including while it is
    still being written or being copied by rsync:

      1. Each frame is its own complete gzip member (see frames.py), so a file
         never depends on a close() to become valid.
      2. A run never appends to a file that an earlier run created. If that run
         was killed mid-write, its file may end in a partial member, and
         anything appended after it would be unreadable. So a restart mid-hour
         starts `-r1`, then `-r2`, and so on.
    """

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._hour_key: str | None = None
        self._path: Path | None = None

    def _fresh_path(self, hour_key: str) -> Path:
        run = 0
        while True:
            path = self.output_dir / f"capture_{hour_key}-r{run}.frames.gz"
            if not path.exists():
                return path
            run += 1

    def write(self, meta: dict, body: bytes) -> None:
        hour_key = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H")
        if hour_key != self._hour_key:
            self._path = self._fresh_path(hour_key)
            self._hour_key = hour_key
            log.info("writing to %s", self._path.name)
        append_frame(self._path, meta, body)


def fetch(session: requests.Session, name: str, url: str, key: str) -> tuple[dict, bytes]:
    """
    Fetch one feed. Never raises.

    Returns (metadata, body). On any failure the body is empty and the metadata
    records what went wrong. We store failures as frames on purpose: later,
    gap analysis needs to tell "the API returned 503" apart from "we were not
    running", and those are very different claims to make in a writeup.
    """
    meta = {
        "feed": name,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "url": url,
    }
    try:
        response = session.get(
            url,
            headers={
                "Ocp-Apim-Subscription-Key": key,
                "User-Agent": USER_AGENT,
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        meta["status"] = response.status_code
        meta["elapsed_ms"] = int(response.elapsed.total_seconds() * 1000)
        if response.status_code != 200:
            # Body of an error response is usually a short JSON explanation.
            # Truncate so a persistent error can't bloat the archive.
            meta["error_body"] = response.text[:500]
            return meta, b""
        meta["bytes"] = len(response.content)
        return meta, response.content
    except Exception as exc:
        meta["status"] = None
        meta["error"] = f"{type(exc).__name__}: {exc}"
        return meta, b""


def main() -> int:
    key = os.environ.get("OC_TRANSPO_PRIMARY_KEY", "").strip()
    if not key:
        log.error("OC_TRANSPO_PRIMARY_KEY is not set. Copy .env.example to .env "
                  "and fill it in, or export it in the environment.")
        return 1

    base_interval = int(os.environ.get("CAPTURE_INTERVAL_SECONDS", str(DEFAULT_INTERVAL)))
    intervals = {
        name: int(os.environ.get(INTERVAL_ENV[name], str(base_interval)))
        for name in FEEDS
    }
    output_dir = Path(os.environ.get("CAPTURE_OUTPUT_DIR", "./raw"))

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    writer = HourlyWriter(output_dir)
    session = requests.Session()

    log.info("capture starting: output=%s intervals=%s",
             output_dir.resolve(),
             ", ".join(f"{n}={intervals[n]}s" for n in FEEDS))

    # Each feed keeps its own schedule against a monotonic clock, so ticks don't
    # drift later and later as request latency accumulates, and so the two feeds
    # can run at different rates.
    next_due = {name: time.monotonic() for name in FEEDS}
    polls = {name: 0 for name in FEEDS}

    try:
        while not _shutdown:
            now_mono = time.monotonic()
            due = [n for n in FEEDS if next_due[n] <= now_mono]

            if due:
                try:
                    for name in due:
                        meta, body = fetch(session, name, FEEDS[name], key)
                        writer.write(meta, body)
                        polls[name] += 1
                        if meta.get("status") != 200:
                            log.warning("%s: status=%s %s", name, meta.get("status"),
                                        meta.get("error") or meta.get("error_body", ""))
                except Exception:
                    # The loop itself must survive anything — a full disk, a
                    # permissions change, a bad clock. Log and take the next tick.
                    log.exception("tick failed")

                for name in due:
                    next_due[name] += intervals[name]
                    if next_due[name] < time.monotonic():
                        # We fell behind (slow API, disk stall). Resync rather
                        # than firing a catch-up burst against a quota.
                        log.warning("%s behind schedule, resyncing", name)
                        next_due[name] = time.monotonic() + intervals[name]

                vp_polls = polls["vehicle_positions"]
                if "vehicle_positions" in due and vp_polls and vp_polls % 120 == 0:
                    log.info("heartbeat: %s", ", ".join(f"{n}={polls[n]}" for n in FEEDS))

            # Wake up often enough to honour a shutdown signal promptly and to
            # notice whichever feed comes due next.
            time.sleep(min(1.0, max(0.05, min(next_due.values()) - time.monotonic())))
    finally:
        session.close()
        log.info("capture stopped after %s", ", ".join(f"{n}={polls[n]} polls" for n in FEEDS))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
