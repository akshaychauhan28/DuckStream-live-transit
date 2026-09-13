"""
Tests for the capture loop's per-feed scheduling.

Runs the real main() loop with the network stubbed out, so the scheduling and
file-writing logic is exercised for real without needing an API key or spending
quota. This is the component that must not fail, so the loop itself is worth
testing rather than just its helpers.
"""

import sys
import threading
import time
from collections import Counter
from pathlib import Path

import pytest

CAPTURE_DIR = Path(__file__).resolve().parent.parent / "capture"
sys.path.insert(0, str(CAPTURE_DIR))

import capture as cap  # noqa: E402
from frames import read_frames  # noqa: E402


@pytest.fixture
def stub_env(monkeypatch, tmp_path):
    """Stub the network and the signal handlers, point output at tmp_path."""
    calls = Counter()

    def fake_fetch(session, name, url, key):
        calls[name] += 1
        return {"feed": name, "status": 200, "seq": calls[name]}, b"stub-body"

    monkeypatch.setattr(cap, "fetch", fake_fetch)
    # signal.signal() only works on the main thread; the loop runs in a worker.
    monkeypatch.setattr(cap.signal, "signal", lambda *a, **k: None)
    monkeypatch.setenv("OC_TRANSPO_PRIMARY_KEY", "test-key")
    monkeypatch.setenv("CAPTURE_OUTPUT_DIR", str(tmp_path))
    # capture.py loads the developer's real .env on import. Clear any interval
    # it set, so each test decides its own schedule.
    for var in ("CAPTURE_INTERVAL_SECONDS", *cap.INTERVAL_ENV.values()):
        monkeypatch.delenv(var, raising=False)
    cap._shutdown = False
    yield calls, tmp_path
    cap._shutdown = False


def _run_for(seconds: float):
    thread = threading.Thread(target=cap.main, daemon=True)
    thread.start()
    time.sleep(seconds)
    cap._shutdown = True
    thread.join(timeout=10)
    assert not thread.is_alive(), "capture loop did not shut down"


def test_feeds_poll_at_their_own_intervals(stub_env, monkeypatch):
    """A slower feed must poll proportionally less often."""
    calls, out = stub_env
    monkeypatch.setenv("CAPTURE_INTERVAL_VEHICLE_POSITIONS_SECONDS", "1")
    monkeypatch.setenv("CAPTURE_INTERVAL_TRIP_UPDATES_SECONDS", "3")

    _run_for(4.5)

    vp, tu = calls["vehicle_positions"], calls["trip_updates"]
    assert vp >= 4, f"expected >=4 vehicle_positions polls, got {vp}"
    assert tu >= 2, f"expected >=2 trip_updates polls, got {tu}"
    assert vp > tu, f"faster feed should poll more often: vp={vp} tu={tu}"


def test_base_interval_applies_to_all_feeds(stub_env, monkeypatch):
    """With no per-feed override, both feeds share CAPTURE_INTERVAL_SECONDS."""
    calls, out = stub_env
    monkeypatch.setenv("CAPTURE_INTERVAL_SECONDS", "1")

    _run_for(3.5)

    vp, tu = calls["vehicle_positions"], calls["trip_updates"]
    assert vp >= 3 and tu >= 3
    assert abs(vp - tu) <= 1, f"feeds should stay in step: vp={vp} tu={tu}"


def test_frames_are_written_and_readable(stub_env, monkeypatch):
    """Everything polled must land on disk in a readable frame."""
    calls, out = stub_env
    monkeypatch.setenv("CAPTURE_INTERVAL_SECONDS", "1")

    _run_for(2.5)

    files = list(Path(out).glob("capture_*.frames.gz"))
    assert files, "no capture file written"

    frames = [f for p in files for f in read_frames(p)]
    assert len(frames) == sum(calls.values()), "every poll should be one frame"
    assert all(body == b"stub-body" for _, body in frames)
    assert {m["feed"] for m, _ in frames} == {"vehicle_positions", "trip_updates"}


def test_restart_never_appends_to_an_earlier_runs_file(tmp_path):
    """
    If a run is killed mid-write, its file can end in a partial gzip member.
    Appending after that would make the rest of the file unreadable, so a new
    run must always start its own file.
    """
    first_run = cap.HourlyWriter(tmp_path)
    first_run.write({"run": 1}, b"a")
    first_run.write({"run": 1}, b"b")

    second_run = cap.HourlyWriter(tmp_path)
    second_run.write({"run": 2}, b"c")

    files = sorted(tmp_path.glob("capture_*.frames.gz"))
    assert len(files) == 2, [f.name for f in files]
    assert [m["run"] for m, _ in read_frames(files[0])] == [1, 1]
    assert [m["run"] for m, _ in read_frames(files[1])] == [2]


def test_file_being_written_is_always_readable(stub_env, monkeypatch):
    """Reading mid-run must never crash — this is what broke inspect_archive."""
    calls, out = stub_env
    monkeypatch.setenv("CAPTURE_INTERVAL_SECONDS", "1")

    thread = threading.Thread(target=cap.main, daemon=True)
    thread.start()
    try:
        time.sleep(2.5)
        files = list(Path(out).glob("capture_*.frames.gz"))
        assert files, "no capture file written"
        stats = {}
        frames = [f for p in files for f in read_frames(p, stats)]  # while running
        assert frames
        assert not stats.get("unreadable_bytes")
    finally:
        cap._shutdown = True
        thread.join(timeout=10)


def test_loop_survives_a_failing_fetch(stub_env, monkeypatch):
    """A throwing fetch must not kill the loop — it must take the next tick."""
    calls, out = stub_env
    monkeypatch.setenv("CAPTURE_INTERVAL_SECONDS", "1")

    original = cap.fetch
    state = {"n": 0}

    def flaky(session, name, url, key):
        state["n"] += 1
        if state["n"] in (1, 2):
            raise RuntimeError("simulated network explosion")
        return original(session, name, url, key)

    monkeypatch.setattr(cap, "fetch", flaky)

    _run_for(3.5)

    assert state["n"] > 3, "loop stopped after the failures instead of continuing"
    files = list(Path(out).glob("capture_*.frames.gz"))
    assert files, "loop never recovered enough to write anything"
