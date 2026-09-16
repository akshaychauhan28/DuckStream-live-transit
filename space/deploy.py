"""
Push the Space to Hugging Face from this repo.

    python space/deploy.py [username/space-name]

The Space's code lives here rather than being edited on the Hub, so the repo
stays the single source of truth and the capture code is shared with the local
setup instead of copy-pasted.

Reads from the environment (or .env):

    HF_TOKEN            a write token — never commit this
    HF_SPACE_REPO_ID    e.g. yourname/duckstream-capture

Run it again after any change to capture/ or space/; the Space rebuilds itself.
"""

import os
import sys
from pathlib import Path

from huggingface_hub import HfApi

ROOT = Path(__file__).resolve().parent.parent
SPACE_DIR = ROOT / "space"
CAPTURE_DIR = ROOT / "capture"

try:
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
except ImportError:
    pass


def main() -> int:
    repo_id = (sys.argv[1] if len(sys.argv) > 1
               else os.environ.get("HF_SPACE_REPO_ID", "")).strip()
    token = os.environ.get("HF_TOKEN", "").strip()

    if not repo_id:
        print("Pass the Space id, e.g. python space/deploy.py yourname/duckstream-capture")
        print("or set HF_SPACE_REPO_ID in .env")
        return 1
    if not token:
        print("HF_TOKEN is not set. Add it to .env (it is gitignored).")
        return 1

    api = HfApi(token=token)

    print(f"Uploading space/ -> {repo_id}")
    api.upload_folder(
        folder_path=SPACE_DIR,
        repo_id=repo_id,
        repo_type="space",
        # deploy.py is a local tool; the Space has no use for it.
        ignore_patterns=["deploy.py", "__pycache__/*", "raw/*"],
        commit_message="Deploy capture from DuckStream repo",
    )

    # The Space imports capture.py and frames.py unchanged, so the same code
    # runs locally and in the Space.
    print(f"Uploading capture/ -> {repo_id}:capture/")
    api.upload_folder(
        folder_path=CAPTURE_DIR,
        path_in_repo="capture",
        repo_id=repo_id,
        repo_type="space",
        allow_patterns=["capture.py", "frames.py"],
        commit_message="Deploy capture from DuckStream repo",
    )

    print(f"\nDone. Watch the build at https://huggingface.co/spaces/{repo_id}")
    print("Status page appears once the build finishes.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
