"""
Tests for the VehiclePositions decoder.

These run against a real captured payload in tests/fixtures/, not a synthetic
one, so the decoder is checked against the field coverage OC Transpo actually
produces — including the ~18% of vehicles with no trip.
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "ingestion"))

from decode import decode_vehicle_positions  # noqa: E402

FIXTURES = sorted((ROOT / "tests" / "fixtures").glob("vehicle_positions_*.pb"))

REQUIRED_FIELDS = {
    "vehicle_id": str,
    "fetched_at": int,
    "feed_timestamp": int,
    "vehicle_timestamp": int,
    "latitude": float,
    "longitude": float,
    "bearing": (float, type(None)),
    "speed": (float, type(None)),
    "trip_id": (str, type(None)),
    "route_id": (str, type(None)),
    "start_time": (str, type(None)),
    "start_date": (str, type(None)),
    "schedule_relationship": (int, type(None)),
    "in_service": bool,
}

FETCHED_AT = "2026-09-13T18:36:54.123456+00:00"


@pytest.fixture(scope="module")
def payload() -> bytes:
    if not FIXTURES:
        pytest.skip("no vehicle_positions fixture — run capture/smoke_test.py")
    return FIXTURES[-1].read_bytes()


@pytest.fixture(scope="module")
def meta() -> dict:
    return {"feed": "vehicle_positions", "fetched_at": FETCHED_AT, "status": 200}


@pytest.fixture(scope="module")
def records(meta, payload) -> list[dict]:
    return decode_vehicle_positions(meta, payload)


def test_one_record_per_vehicle_entity(records, payload):
    """Every vehicle in the response becomes exactly one record."""
    from google.transit import gtfs_realtime_pb2

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(payload)
    expected = sum(1 for e in feed.entity if e.HasField("vehicle"))

    assert len(records) == expected
    assert expected > 100, "fixture looks too small to be a real payload"


def test_every_record_has_the_full_schema(records):
    """Missing keys break Parquet later, so every record carries every field."""
    for record in records:
        assert set(record) == set(REQUIRED_FIELDS), "field names must match exactly"
        for field, expected_type in REQUIRED_FIELDS.items():
            assert isinstance(record[field], expected_type), (
                f"{field}={record[field]!r} is {type(record[field]).__name__}, "
                f"expected {expected_type}"
            )


def test_positions_are_plausible_for_ottawa(records):
    for record in records:
        assert 44.0 < record["latitude"] < 46.5, record["latitude"]
        assert -77.0 < record["longitude"] < -74.0, record["longitude"]


def test_timestamps_are_epoch_seconds(records):
    """Seconds, not milliseconds, and not datetime objects."""
    expected_fetched = int(datetime.fromisoformat(FETCHED_AT).timestamp())
    for record in records:
        assert record["fetched_at"] == expected_fetched
        # Any sane epoch-seconds value for this project.
        for field in ("feed_timestamp", "vehicle_timestamp"):
            assert 1_600_000_000 < record[field] < 2_000_000_000, (
                f"{field}={record[field]} — milliseconds by mistake?"
            )


def test_feed_timestamp_is_the_same_for_every_record(records, payload):
    """It comes from the response header, so one value applies to the whole poll."""
    from google.transit import gtfs_realtime_pb2

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(payload)

    assert {r["feed_timestamp"] for r in records} == {feed.header.timestamp}


def test_missing_trip_becomes_null_not_empty_string(records):
    """Protobuf returns "" for absent strings; that is a default, not a value."""
    for record in records:
        for field in ("trip_id", "route_id", "start_time", "start_date"):
            assert record[field] != "", f"{field} should be None, not an empty string"


def test_in_service_matches_whether_a_trip_is_present(records):
    for record in records:
        if record["in_service"]:
            assert record["trip_id"], "in_service records must carry a trip_id"
        else:
            assert record["trip_id"] is None
            assert record["route_id"] is None


def test_out_of_service_vehicles_are_kept(records):
    """
    Roughly 18% of vehicles have a position but no trip. Dropping them would
    silently distort every fleet count, so the decoder keeps them.
    """
    out_of_service = [r for r in records if not r["in_service"]]
    assert out_of_service, "no out-of-service vehicles — are they being dropped?"

    for record in out_of_service:
        assert record["vehicle_id"]
        assert isinstance(record["latitude"], float)


def test_vehicle_ids_are_unique_within_one_poll(records):
    ids = [r["vehicle_id"] for r in records]
    assert len(ids) == len(set(ids)), "the same vehicle appeared twice in one response"


def test_failed_poll_decodes_to_nothing():
    """Failed frames are stored for gap analysis but contain no vehicles."""
    assert decode_vehicle_positions(
        {"feed": "vehicle_positions", "fetched_at": FETCHED_AT, "status": 503}, b""
    ) == []
    assert decode_vehicle_positions(
        {"feed": "vehicle_positions", "fetched_at": FETCHED_AT, "status": None}, b""
    ) == []


def test_speed_of_zero_is_not_confused_with_missing(records):
    """A stopped bus and an unknown speed are different facts."""
    speeds = [r["speed"] for r in records]
    assert any(s == 0.0 for s in speeds), "expected some stationary vehicles"
    assert all(s is None or isinstance(s, float) for s in speeds)
