"""
Framing format for the raw capture archive.

Each captured poll is stored as two length-prefixed blocks inside an
hourly gzip file:

    [4-byte BE length][UTF-8 JSON metadata][4-byte BE length][raw response body]

Why this format rather than one-file-per-poll or base64 JSONL:

  - One file per poll would mean ~5,760 tiny files per day. That is the same
    small-file problem we're avoiding in the Parquet layer, and it makes rsync
    crawl.
  - Base64-in-JSONL is self-describing and easy, but base64 obscures the byte
    patterns gzip relies on, so the archive ends up meaningfully larger over
    30 days. Storage is the one resource we're actually tight on.
  - Length-prefixed binary keeps the protobuf bytes intact and compressible,
    while the JSON metadata block keeps every frame self-describing (when we
    fetched it, from which endpoint, what the HTTP status was).

The metadata block is what makes gap analysis possible later: a frame with
status 503 and an empty body means "we polled and the API failed", which is a
different fact from "we never polled". Both matter for honest uptime reporting.
"""

import gzip
import json
import struct
from pathlib import Path
from typing import Iterator

# 4-byte big-endian unsigned int. Caps a single block at 4GB, which is
# several orders of magnitude more headroom than a GTFS-RT response needs.
_LEN = struct.Struct(">I")


def write_frame(fh, meta: dict, body: bytes) -> None:
    """Append one frame to an open binary file handle (typically gzip)."""
    meta_bytes = json.dumps(meta, separators=(",", ":"), sort_keys=True).encode("utf-8")
    fh.write(_LEN.pack(len(meta_bytes)))
    fh.write(meta_bytes)
    fh.write(_LEN.pack(len(body)))
    fh.write(body)


def read_frames(path: str | Path) -> Iterator[tuple[dict, bytes]]:
    """
    Yield (metadata, body) for every frame in a capture file.

    Tolerates a truncated final frame. That is not a hypothetical: if the VM is
    killed or loses power mid-write, the last frame will be partial. We'd rather
    return the 119 good frames in the file than raise and lose the hour.
    """
    with gzip.open(path, "rb") as fh:
        while True:
            head = fh.read(4)
            if len(head) < 4:
                return  # clean EOF, or a truncated header we can't use

            (meta_len,) = _LEN.unpack(head)
            meta_raw = fh.read(meta_len)
            if len(meta_raw) < meta_len:
                return  # truncated mid-metadata

            body_head = fh.read(4)
            if len(body_head) < 4:
                return  # truncated between metadata and body

            (body_len,) = _LEN.unpack(body_head)
            body = fh.read(body_len)
            if len(body) < body_len:
                return  # truncated mid-body

            yield json.loads(meta_raw.decode("utf-8")), body
