"""
Copy capture files from the phone to this machine over the local network.

    python scripts/pull_from_phone.py [http://<phone-ip>:8000]

The phone serves its raw/ folder with a plain HTTP server; this pulls anything
missing or changed into the local raw/ folder. Safe to run on a schedule: it
skips files it already has, re-fetches only the hour still being written, and
exits quietly when the phone isn't reachable.

Pull rather than push, because the laptop sleeps. A push from the phone would
fail whenever the laptop was asleep and need retry logic; pulling means the
laptop simply catches up whenever it happens to be awake.

Set PHONE_RAW_URL in .env to avoid passing the address every time.
"""

import os
import re
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEST = Path(os.environ.get("CAPTURE_OUTPUT_DIR", ROOT / "raw"))
LOG = ROOT / "logs" / "pull.log"
TIMEOUT = 20


def record(message: str) -> None:
    """Print, and append to a log — a scheduled run has no console to watch."""
    print(message)
    try:
        LOG.parent.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        with LOG.open("a", encoding="utf-8") as fh:
            fh.write(f"{stamp}  {message}\n")
    except OSError:
        pass  # never let logging break the copy

try:
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
except ImportError:
    pass

# Python's http.server returns an HTML directory index; the links are the names.
LINK = re.compile(r'href="([^"?]+\.gz)"')


def remote_files(base: str) -> list[str]:
    with urllib.request.urlopen(base, timeout=TIMEOUT) as response:
        html = response.read().decode("utf-8", "replace")
    return sorted(set(LINK.findall(html)))


def remote_size(url: str) -> int | None:
    """Ask for the size without downloading. None if the server won't say."""
    request = urllib.request.Request(url, method="HEAD")
    with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
        length = response.headers.get("Content-Length")
    return int(length) if length is not None else None


def download(url: str, target: Path) -> int:
    """Download to a temporary name, then rename, so a partial file is never
    mistaken for a complete one if this is interrupted."""
    temp = target.with_suffix(target.suffix + ".part")
    with urllib.request.urlopen(url, timeout=TIMEOUT) as response, temp.open("wb") as out:
        copied = 0
        while chunk := response.read(256 * 1024):
            out.write(chunk)
            copied += len(chunk)
    temp.replace(target)
    return copied


def main() -> int:
    base = (sys.argv[1] if len(sys.argv) > 1
            else os.environ.get("PHONE_RAW_URL", "")).strip().rstrip("/")
    if not base:
        print("Pass the phone's address, e.g.")
        print("    python scripts/pull_from_phone.py http://192.168.1.5:8000")
        print("or set PHONE_RAW_URL in .env")
        return 1

    DEST.mkdir(parents=True, exist_ok=True)

    try:
        names = remote_files(base)
    except (urllib.error.URLError, OSError) as exc:
        # Phone asleep, off Wi-Fi, or server not running. Not an error worth
        # shouting about on a schedule — there will be another run.
        record(f"phone not reachable at {base} ({exc})")
        return 0

    if not names:
        record(f"no capture files at {base}")
        return 0

    new = updated = skipped = 0
    total_bytes = 0

    for name in names:
        url = f"{base}/{name}"
        target = DEST / name
        try:
            size = remote_size(url)
        except (urllib.error.URLError, OSError) as exc:
            print(f"  ! {name}: {exc}")
            continue

        if target.exists():
            # The hour currently being written keeps growing, so its size
            # changes between runs and it gets fetched again. Completed hours
            # match and are skipped.
            if size is not None and target.stat().st_size == size:
                skipped += 1
                continue
            action, label = "updated", "~"
        else:
            action, label = "new", "+"

        try:
            copied = download(url, target)
        except (urllib.error.URLError, OSError) as exc:
            print(f"  ! {name}: {exc}")
            continue

        total_bytes += copied
        if action == "new":
            new += 1
        else:
            updated += 1
        print(f"  {label} {name}  ({copied/1e6:,.1f} MB)")

    # Plain ASCII: this line lands in a log file that gets read by tools with
    # their own ideas about encoding (PowerShell 5.1 reads as ANSI by default).
    record(f"{new} new, {updated} updated, {skipped} already current "
           f"- {total_bytes/1e6:,.1f} MB transferred into {DEST}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
