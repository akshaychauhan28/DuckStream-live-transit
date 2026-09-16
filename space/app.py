"""
Hugging Face Space entry point.

Three things run here:

  1. capture.py, unchanged, polling the OC Transpo feeds into a local folder.
  2. A CommitScheduler, uploading that folder to a private Dataset repo every
     few minutes. A Space's disk is wiped whenever it restarts, so anything not
     yet uploaded is lost — the upload interval is the size of that window.
  3. A small status page on port 7860. A Space with no web server is treated as
     broken, and free Spaces sleep after 48 hours with no visitors, so the page
     is also what a keep-alive ping has to hit.

Configuration comes from the Space's Variables and Secrets:

    OC_TRANSPO_PRIMARY_KEY  secret    the API key
    HF_TOKEN                secret    write token for the dataset
    DATASET_REPO_ID         variable  e.g. yourname/duckstream-raw
    UPLOAD_EVERY_MINUTES    variable  optional, default 5
"""

import os
import sys
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from huggingface_hub import CommitScheduler

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "capture"))

import capture  # noqa: E402

RAW = Path(os.environ.get("CAPTURE_OUTPUT_DIR", str(HERE / "raw")))
STARTED_AT = datetime.now(timezone.utc)
STATE = {"error": None, "uploading_to": None, "every_minutes": None}


class StatusHandler(BaseHTTPRequestHandler):
    """Plain-text status page. Also what the keep-alive ping hits."""

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(self._status().encode("utf-8"))

    def _status(self) -> str:
        now = datetime.now(timezone.utc)
        files = sorted(RAW.glob("capture_*.gz")) if RAW.exists() else []
        total = sum(f.stat().st_size for f in files)
        newest = max((f.stat().st_mtime for f in files), default=None)

        lines = [
            "DuckStream capture",
            "",
            f"started      {STARTED_AT.isoformat()}",
            f"now          {now.isoformat()}",
            f"uptime       {(now - STARTED_AT).total_seconds() / 3600:.2f} h",
            "",
            f"files        {len(files)}",
            f"on disk      {total / 1e6:,.1f} MB (wiped on restart)",
        ]
        if newest is not None:
            age = now.timestamp() - newest
            lines.append(f"last write   {age:,.0f}s ago")
            # Anything beyond a couple of poll intervals means capture stalled.
            lines.append(f"healthy      {'yes' if age < 300 else 'NO — capture may have stalled'}")
        else:
            lines.append("last write   nothing written yet")

        lines += [
            "",
            f"uploading to {STATE['uploading_to']} every {STATE['every_minutes']} min",
        ]
        if STATE["error"]:
            lines += ["", f"ERROR        {STATE['error']}"]
        return "\n".join(lines) + "\n"

    def log_message(self, *args):
        """Silence per-request logging; keep-alive pings would flood the logs."""


def serve_status():
    port = int(os.environ.get("PORT", "7860"))
    ThreadingHTTPServer(("0.0.0.0", port), StatusHandler).serve_forever()


def main() -> int:
    repo_id = os.environ.get("DATASET_REPO_ID", "").strip()
    every = int(os.environ.get("UPLOAD_EVERY_MINUTES", "5"))
    STATE["uploading_to"] = repo_id or "(DATASET_REPO_ID not set)"
    STATE["every_minutes"] = every

    # Start the status page first, so configuration problems are visible on the
    # Space's page rather than only in the build logs.
    threading.Thread(target=serve_status, daemon=True).start()

    if not repo_id:
        STATE["error"] = "DATASET_REPO_ID is not set — nothing would be saved, so not starting capture."
        threading.Event().wait()
        return 1
    if not os.environ.get("HF_TOKEN"):
        STATE["error"] = "HF_TOKEN is not set — uploads would fail, so not starting capture."
        threading.Event().wait()
        return 1

    RAW.mkdir(parents=True, exist_ok=True)

    # Uploads run in their own background thread and retry on the next tick if
    # one fails, so a network blip costs nothing. Files are only ever appended
    # to, which is what CommitScheduler expects.
    CommitScheduler(
        repo_id=repo_id,
        repo_type="dataset",
        folder_path=RAW,
        path_in_repo="raw",
        every=every,
        allow_patterns=["capture_*.gz"],
    )

    # capture.main() installs signal handlers, so it has to own the main thread.
    return capture.main()


if __name__ == "__main__":
    raise SystemExit(main())
