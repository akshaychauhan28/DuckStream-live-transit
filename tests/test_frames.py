"""
Tests for the raw archive framing format.

The truncation tests matter most, and they cut the *compressed* bytes on disk.
An earlier version of these tests cut the decompressed stream and re-compressed
it, which always produces a valid gzip file, so it never exercised the failure
it claimed to cover. It passed while reading a live capture file crashed.
"""

import gzip
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "capture"))
from frames import append_frame, encode_frame, read_frames  # noqa: E402


def _seqs(frames):
    return [meta["seq"] for meta, _ in frames]


def test_roundtrip_single_frame(tmp_path):
    path = tmp_path / "one.frames.gz"
    meta = {"feed": "vehicle_positions", "status": 200}
    body = b"\x1a\x2b\x00\xff binary payload"

    append_frame(path, meta, body)

    assert list(read_frames(path)) == [(meta, body)]


def test_roundtrip_many_frames_preserves_order(tmp_path):
    path = tmp_path / "many.frames.gz"
    expected = [({"feed": "f", "seq": i}, bytes([i % 256]) * (i * 7)) for i in range(50)]

    for meta, body in expected:
        append_frame(path, meta, body)

    stats = {}
    assert list(read_frames(path, stats)) == expected
    assert stats == {}, "a cleanly written file should report nothing unusual"


def test_empty_body_is_preserved(tmp_path):
    """Failed polls are stored as frames with no body — that's a real record."""
    path = tmp_path / "failed.frames.gz"
    meta = {"feed": "trip_updates", "status": 503, "error_body": "upstream down"}

    append_frame(path, meta, b"")

    assert list(read_frames(path)) == [(meta, b"")]


def test_files_are_plain_multi_member_gzip(tmp_path):
    """Standard tools (gzip -dc, zcat) must be able to read an archive file."""
    path = tmp_path / "std.frames.gz"
    frames = [({"seq": i}, b"body") for i in range(3)]
    for meta, body in frames:
        append_frame(path, meta, body)

    assert gzip.decompress(path.read_bytes()) == b"".join(encode_frame(m, b) for m, b in frames)


def test_cut_off_file_returns_every_complete_frame(tmp_path):
    """A writer killed mid-write leaves a partial gzip member at the end."""
    path = tmp_path / "whole.frames.gz"
    originals = [({"seq": i}, f"payload-{i}".encode() * 40) for i in range(5)]
    for meta, body in originals:
        append_frame(path, meta, body)
    raw = path.read_bytes()
    last_member = len(gzip.compress(encode_frame(*originals[-1])))

    # Cut inside the last member's trailer, its compressed data, and its header.
    for cut in (1, 4, 8, 12, last_member // 2, last_member - 3):
        cut_path = tmp_path / f"cut_{cut}.frames.gz"
        cut_path.write_bytes(raw[:-cut])

        stats = {}
        frames = list(read_frames(cut_path, stats))  # must not raise

        assert frames[:4] == originals[:4], f"lost complete frames at cut={cut}"
        assert frames == originals[:len(frames)], f"returned a corrupted frame at cut={cut}"
        assert stats.get("incomplete_tail"), f"didn't flag the cut at cut={cut}"


def test_legacy_file_still_being_written_is_readable(tmp_path):
    """
    The first capture.py kept one gzip stream open for the hour and only
    flushed. gzip writes its end marker on close, so the current hour's file had
    none — which is exactly the file that crashed the inspector.
    """
    path = tmp_path / "capture_2026-09-13T18.jsonl.gz"
    fh = gzip.open(path, "ab")
    try:
        for i in range(3):
            fh.write(encode_frame({"seq": i}, b"x" * 100))
        fh.flush()

        stats = {}
        assert _seqs(read_frames(path, stats)) == [0, 1, 2]
        assert stats.get("incomplete_tail")
    finally:
        fh.close()

    stats = {}
    assert _seqs(read_frames(path, stats)) == [0, 1, 2]
    assert stats == {}, "once closed, the legacy file is complete"


def test_legacy_restart_after_kill_keeps_frames_before_the_damage(tmp_path):
    """
    Worst case under the old writer: a run is killed without closing its file,
    and the next run appends to the same file. What the first run wrote must
    survive, and the damage must be reported rather than raised.
    """
    path = tmp_path / "capture_2026-09-13T18.jsonl.gz"

    fh = gzip.open(path, "ab")
    for i in range(3):
        fh.write(encode_frame({"seq": i}, b"x" * 100))
    fh.flush()
    left_by_killed_run = path.read_bytes()
    fh.close()
    path.write_bytes(left_by_killed_run)

    with gzip.open(path, "ab") as next_run:
        next_run.write(encode_frame({"seq": 99}, b"y"))

    stats = {}
    assert _seqs(read_frames(path, stats)) == [0, 1, 2]
    assert stats.get("unreadable_bytes", 0) > 0
