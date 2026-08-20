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

import gzip
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from frames import write_frame  # noqa: E402

# --- Feeds -----------------------------------------------------------------
# Confirmed against the OC Transpo developer portal (Azure API Management).
# Auth is a subscription key in the Ocp-Apim-Subscription-Key header.
# Both default to protobuf; we store the bytes exactly as returned.
FEEDS = {
    "vehicle_positions": "https://nextrip-public-api.azure-api.net/octranspo/gtfs-rt-vp/beta/v1/VehiclePositions",
    "trip_updates": "https://nextrip-public-api.azure-api.net/octranspo/gtfs-rt-tp/beta/v1/TripUpdates",
}

REQUEST_TIMEOUT_SECONDS = 20

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
    Holds one open gzip file per UTC hour and rolls over on the hour boundary.

    Kept as a class purely so the rollover logic and the file handle live in one
    place — if a rollover throws, we want it to be obvious where.
    """

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._hour_key: str | None = None
        self._fh = None

    def _path_for(self, hour_key: str) -> Path:
        # Flat filenames sort chronologically and rsync cheaply.
        return self.output_dir / f"capture_{hour_key}.jsonl.gz"

    def handle(self, now: datetime):
        hour_key = now.strftime("%Y-%m-%dT%H")
        if hour_key != self._hour_key:
            self.close()
            path = self._path_for(hour_key)
            # Append mode: if the process restarts mid-hour we add to the
            # existing file rather than truncating an hour of collection.
            self._fh = gzip.open(path, "ab")
            self._hour_key = hour_key
            log.info("writing to %s", path.name)
        return self._fh

    def close(self):
        if self._fh is not None:
            try:
                self._fh.close()
            except Exception:
                log.exception("failed closing capture file")
            self._fh = None


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
            headers={"Ocp-Apim-Subscription-Key": key},
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

    interval = int(os.environ.get("CAPTURE_INTERVAL_SECONDS", "30"))
    output_dir = Path(os.environ.get("CAPTURE_OUTPUT_DIR", "./raw"))

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    writer = HourlyWriter(output_dir)
    session = requests.Session()

    log.info("capture starting: interval=%ss output=%s feeds=%s",
             interval, output_dir.resolve(), ", ".join(FEEDS))

    # Schedule against a monotonic clock so ticks don't drift later and later
    # as request latency accumulates.
    next_tick = time.monotonic()
    ticks = 0

    try:
        while not _shutdown:
            now = datetime.now(timezone.utc)
            try:
                fh = writer.handle(now)
                for name, url in FEEDS.items():
                    meta, body = fetch(session, name, url, key)
                    write_frame(fh, meta, body)
                    if meta.get("status") != 200:
                        log.warning("%s: status=%s %s", name, meta.get("status"),
                                    meta.get("error") or meta.get("error_body", ""))
                fh.flush()
            except Exception:
                # The loop itself must survive anything — a full disk, a
                # permissions change, a bad clock. Log it and take the next tick.
                log.exception("tick failed")

            ticks += 1
            if ticks % 120 == 0:  # roughly hourly at a 30s interval
                log.info("heartbeat: %s ticks completed", ticks)

            next_tick += interval
            sleep_for = next_tick - time.monotonic()
            if sleep_for < 0:
                # We fell behind (slow API, disk stall). Resync rather than
                # trying to catch up with a burst of requests against a quota.
                log.warning("behind schedule by %.1fs, resyncing", -sleep_for)
                next_tick = time.monotonic()
                sleep_for = 0
            # Sleep in short slices so a shutdown signal is honoured promptly.
            deadline = time.monotonic() + sleep_for
            while not _shutdown and time.monotonic() < deadline:
                time.sleep(min(1.0, deadline - time.monotonic()))
    finally:
        writer.close()
        session.close()
        log.info("capture stopped after %s ticks", ticks)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
