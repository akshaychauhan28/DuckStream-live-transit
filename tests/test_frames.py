"""
Tests for the raw archive framing format.

The truncation test is the important one. If the VM loses power mid-write, the
last frame in that hour's file will be partial — and we need the reader to
return the good frames rather than raising and costing us the whole hour.
"""

import gzip
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "capture"))
from frames import read_frames, write_frame  # noqa: E402


def test_roundtrip_single_frame(tmp_path):
    path = tmp_path / "one.jsonl.gz"
    meta = {"feed": "vehicle_positions", "status": 200}
    body = b"\x1a\x2b\x00\xff binary payload"

    with gzip.open(path, "ab") as fh:
        write_frame(fh, meta, body)

    frames = list(read_frames(path))
    assert len(frames) == 1
    assert frames[0][0] == meta
    assert frames[0][1] == body


def test_roundtrip_many_frames_preserves_order(tmp_path):
    path = tmp_path / "many.jsonl.gz"
    expected = [({"feed": "f", "seq": i}, bytes([i % 256]) * (i * 7)) for i in range(50)]

    with gzip.open(path, "ab") as fh:
        for meta, body in expected:
            write_frame(fh, meta, body)

    assert list(read_frames(path)) == expected


def test_empty_body_is_preserved(tmp_path):
    """Failed polls are stored as frames with no body — that's a real record."""
    path = tmp_path / "failed.jsonl.gz"
    meta = {"feed": "trip_updates", "status": 503, "error_body": "upstream down"}

    with gzip.open(path, "ab") as fh:
        write_frame(fh, meta, b"")

    frames = list(read_frames(path))
    assert frames == [(meta, b"")]


def test_append_across_reopens(tmp_path):
    """Restarting mid-hour must append, not truncate the hour's collection."""
    path = tmp_path / "append.jsonl.gz"

    for i in range(3):
        with gzip.open(path, "ab") as fh:
            write_frame(fh, {"seq": i}, b"x")

    assert [m["seq"] for m, _ in read_frames(path)] == [0, 1, 2]


def test_truncated_final_frame_yields_the_good_ones(tmp_path):
    """Power loss mid-write must not cost us the frames already written."""
    good = tmp_path / "good.jsonl.gz"
    with gzip.open(good, "ab") as fh:
        for i in range(5):
            write_frame(fh, {"seq": i}, b"payload")

    raw = gzip.decompress(good.read_bytes())

    # Chop the stream at several points inside what would be the last frame.
    for cut in (len(raw) - 1, len(raw) - 5, len(raw) - 12):
        truncated = tmp_path / f"cut_{cut}.jsonl.gz"
        truncated.write_bytes(gzip.compress(raw[:cut]))

        frames = list(read_frames(truncated))  # must not raise
        assert len(frames) >= 4
        assert [m["seq"] for m, _ in frames] == list(range(len(frames)))
