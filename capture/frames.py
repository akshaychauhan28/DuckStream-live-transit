"""
Framing format for the raw capture archive.

Each captured poll becomes one frame:

    [4-byte BE length][UTF-8 JSON metadata][4-byte BE length][raw response body]

and each frame is compressed as its own complete gzip member, appended to the
current file. A capture file is therefore just complete gzip members glued end
to end — which standard tools (`gzip -dc`, `zcat`) read natively.

Why one gzip member per frame, rather than one long gzip stream per file:

  - A gzip stream only gets its end-of-stream marker when the file is closed.
    Holding a file open for an hour means the file being written is never a
    valid gzip file until the hour ends, and anything that reads it early —
    an inspection script, or rsync copying it to the laptop — sees a stream
    that ends without its marker.
  - With per-frame members, every frame on disk is complete the moment its
    write returns. The only thing a crash can leave behind is one partial
    member at the very end of the file.

Why length-prefixed binary rather than one-file-per-poll or base64 JSONL:

  - One file per poll would mean ~3,600 tiny files per day. That is the same
    small-file problem we're avoiding in the Parquet layer, and it makes rsync
    crawl.
  - Base64 obscures the byte patterns gzip relies on, so the archive would be
    meaningfully larger over 30 days.

The metadata block is what makes gap analysis possible later: a frame with
status 503 and an empty body means "we polled and the API failed", which is a
different fact from "we never polled". Both matter for honest uptime reporting.
"""

import gzip
import json
import struct
import zlib
from pathlib import Path
from typing import Iterator

# 4-byte big-endian unsigned int. Caps a single block at 4GB, which is
# several orders of magnitude more headroom than a GTFS-RT response needs.
_LEN = struct.Struct(">I")

# zlib's wbits=31 means "expect a gzip header and trailer".
_GZIP_WBITS = 31
_READ_CHUNK = 64 * 1024


def encode_frame(meta: dict, body: bytes) -> bytes:
    """Serialise one frame, uncompressed."""
    meta_bytes = json.dumps(meta, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return _LEN.pack(len(meta_bytes)) + meta_bytes + _LEN.pack(len(body)) + body


def append_frame(path: str | Path, meta: dict, body: bytes) -> None:
    """
    Append one frame to `path` as a complete gzip member.

    The member is fully built in memory and written in a single call, so the
    only way to leave a partial member on disk is to be killed mid-write.
    """
    member = gzip.compress(encode_frame(meta, body))
    with open(path, "ab") as fh:
        fh.write(member)


def read_frames(path: str | Path, stats: dict | None = None) -> Iterator[tuple[dict, bytes]]:
    """
    Yield (metadata, body) for every complete frame in a capture file.

    Never raises on a damaged or unfinished file. Instead it returns every frame
    it can, and if a `stats` dict is passed, records why it stopped:

      stats["incomplete_tail"] = True
          The file ends mid-frame or mid-gzip-member. Normal for a file that
          is still being written, or one whose writer was killed.

      stats["unreadable_bytes"] = N
          Decompression hit invalid data N bytes from the end and gave up.
          Should not happen with files written by append_frame; it can happen
          with files from the earlier writer if a run was killed and a new run
          appended to the same file.
    """
    if stats is None:
        stats = {}
    raw = Path(path).read_bytes()

    for member in _gzip_members(raw, stats):
        yield from _parse_frames(member, stats)


def _gzip_members(raw: bytes, stats: dict) -> Iterator[bytes]:
    """Yield the decompressed contents of each gzip member in `raw`, in order."""
    view = memoryview(raw)
    pos = 0

    while pos < len(raw):
        inflater = zlib.decompressobj(_GZIP_WBITS)
        out = bytearray()
        while pos < len(raw) and not inflater.eof:
            chunk = view[pos:pos + _READ_CHUNK]
            checkpoint = inflater.copy()
            try:
                out += inflater.decompress(chunk)
            except zlib.error:
                # zlib discards a call's output when it raises, so replay this
                # chunk from the checkpoint to keep what decoded before the
                # damage. Then stop: nothing after it can be trusted to line up
                # with frame boundaries.
                salvaged, good = _decode_until_error(checkpoint, chunk)
                out += salvaged
                stats["unreadable_bytes"] = len(raw) - (pos + good)
                yield bytes(out)
                return
            pos += len(chunk)

        if inflater.eof:
            # The chunk we fed may have run past this member into the next one.
            pos -= len(inflater.unused_data)
        else:
            # Ran out of file before the member's end marker.
            stats["incomplete_tail"] = True

        yield bytes(out)


def _decode_until_error(inflater, chunk) -> tuple[bytes, int]:
    """
    Feed a damaged chunk one byte at a time.

    Returns the output produced before the first bad byte, and how many bytes
    of the chunk were accepted. Slow, but only ever runs on the damage path.
    """
    out = bytearray()
    for i in range(len(chunk)):
        try:
            out += inflater.decompress(chunk[i:i + 1])
        except zlib.error:
            return bytes(out), i
    return bytes(out), len(chunk)


def _parse_frames(buf: bytes, stats: dict) -> Iterator[tuple[dict, bytes]]:
    """Split one member's decompressed bytes into frames."""
    pos = 0
    end = len(buf)

    while pos < end:
        if end - pos < _LEN.size:
            stats["incomplete_tail"] = True
            return
        (meta_len,) = _LEN.unpack_from(buf, pos)
        pos += _LEN.size

        if end - pos < meta_len + _LEN.size:
            stats["incomplete_tail"] = True
            return
        meta_raw = buf[pos:pos + meta_len]
        pos += meta_len

        (body_len,) = _LEN.unpack_from(buf, pos)
        pos += _LEN.size

        if end - pos < body_len:
            stats["incomplete_tail"] = True
            return
        body = buf[pos:pos + body_len]
        pos += body_len

        try:
            meta = json.loads(meta_raw.decode("utf-8"))
        except ValueError:
            stats["unreadable_bytes"] = stats.get("unreadable_bytes", 0) + (end - pos)
            return

        yield meta, body
