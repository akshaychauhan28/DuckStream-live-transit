"""
Tests for the TripUpdates decoder.

Run against a real captured payload, so the decoder is checked against what OC
Transpo actually sends — including the cancelled trips that carry no
predictions at all, which are the reason this returns two lists instead of one.
"""

import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "ingestion"))

from decode import decode_trip_updates  # noqa: E402

FIXTURES = sorted((ROOT / "tests" / "fixtures").glob("trip_updates_*.pb"))
FETCHED_AT = "2026-09-13T18:36:54.123456+00:00"

TRIP_FIELDS = {
    "trip_id": str,
    "start_date": (str, type(None)),
    "start_time": (str, type(None)),
    "route_id": (str, type(None)),
    "schedule_relationship": int,
    "vehicle_id": (str, type(None)),
    "trip_update_timestamp": (int, type(None)),
    "prediction_count": int,
    "fetched_at": int,
    "feed_timestamp": int,
}

PREDICTION_FIELDS = {
    "trip_id": str,
    "start_date": (str, type(None)),
    "stop_sequence": int,
    "stop_id": (str, type(None)),
    "arrival_time": (int, type(None)),
    "departure_time": (int, type(None)),
    "schedule_relationship": int,
    "fetched_at": int,
    "feed_timestamp": int,
}


@pytest.fixture(scope="module")
def payload() -> bytes:
    if not FIXTURES:
        pytest.skip("no trip_updates fixture — run capture/smoke_test.py")
    return FIXTURES[-1].read_bytes()


@pytest.fixture(scope="module")
def meta() -> dict:
    return {"feed": "trip_updates", "fetched_at": FETCHED_AT, "status": 200}


@pytest.fixture(scope="module")
def decoded(meta, payload):
    return decode_trip_updates(meta, payload)


@pytest.fixture(scope="module")
def trips(decoded):
    return decoded[0]


@pytest.fixture(scope="module")
def predictions(decoded):
    return decoded[1]


def test_returns_two_lists(decoded):
    assert isinstance(decoded, tuple) and len(decoded) == 2
    assert all(isinstance(part, list) for part in decoded)


def test_one_trip_row_per_trip_in_the_response(trips, payload):
    from google.transit import gtfs_realtime_pb2

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(payload)
    expected = sum(1 for e in feed.entity if e.HasField("trip_update"))

    assert len(trips) == expected
    assert expected > 100, "fixture looks too small to be a real payload"


def test_one_prediction_row_per_stop_time_update(predictions, payload):
    from google.transit import gtfs_realtime_pb2

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(payload)
    expected = sum(len(e.trip_update.stop_time_update)
                   for e in feed.entity if e.HasField("trip_update"))

    assert len(predictions) == expected
    assert expected > len(set(p["trip_id"] for p in predictions)), (
        "expected many predictions per trip"
    )


def test_trip_rows_have_the_full_schema(trips):
    for trip in trips:
        assert set(trip) == set(TRIP_FIELDS), "trip field names must match exactly"
        for field, expected_type in TRIP_FIELDS.items():
            assert isinstance(trip[field], expected_type), (
                f"trip.{field}={trip[field]!r} is {type(trip[field]).__name__}"
            )


def test_prediction_rows_have_the_full_schema(predictions):
    for prediction in predictions:
        assert set(prediction) == set(PREDICTION_FIELDS), "prediction fields must match"
        for field, expected_type in PREDICTION_FIELDS.items():
            assert isinstance(prediction[field], expected_type), (
                f"prediction.{field}={prediction[field]!r} is "
                f"{type(prediction[field]).__name__}"
            )


def test_cancelled_trips_survive_with_no_predictions(trips, predictions):
    """
    The whole reason for two lists. A cancelled trip has nothing to predict, so
    flattening to predictions alone would erase it.
    """
    cancelled = [t for t in trips if t["schedule_relationship"] == 3]
    assert cancelled, "no cancelled trips in this fixture — try another capture"

    keys_with_predictions = {(p["trip_id"], p["start_date"]) for p in predictions}
    for trip in cancelled:
        assert trip["prediction_count"] == 0
        assert (trip["trip_id"], trip["start_date"]) not in keys_with_predictions


def test_prediction_count_matches_the_predictions_returned(trips, predictions):
    counted = Counter((p["trip_id"], p["start_date"]) for p in predictions)
    for trip in trips:
        key = (trip["trip_id"], trip["start_date"])
        assert trip["prediction_count"] == counted.get(key, 0), (
            f"prediction_count wrong for {key}"
        )


def test_every_prediction_belongs_to_a_trip(trips, predictions):
    """Referential integrity: no orphan predictions."""
    trip_keys = {(t["trip_id"], t["start_date"]) for t in trips}
    for prediction in predictions:
        assert (prediction["trip_id"], prediction["start_date"]) in trip_keys


def test_no_duplicate_stop_sequence_within_a_trip(predictions):
    """(trip, stop_sequence) is the primary key in GTFS, so it can't repeat."""
    seen = Counter(
        (p["trip_id"], p["start_date"], p["stop_sequence"]) for p in predictions
    )
    repeated = [key for key, n in seen.items() if n > 1]
    assert not repeated, f"duplicate stop_sequence: {repeated[:3]}"


def test_timestamps_are_epoch_seconds(trips, predictions):
    expected_fetched = int(datetime.fromisoformat(FETCHED_AT).timestamp())

    for trip in trips:
        assert trip["fetched_at"] == expected_fetched
        assert 1_600_000_000 < trip["feed_timestamp"] < 2_000_000_000

    times = [p["arrival_time"] for p in predictions if p["arrival_time"] is not None]
    assert times, "expected some arrival predictions"
    for value in times:
        assert 1_600_000_000 < value < 2_000_000_000, (
            f"arrival_time={value} — milliseconds by mistake?"
        )


def test_empty_strings_became_null(trips, predictions):
    for trip in trips:
        for field in ("start_date", "start_time", "route_id", "vehicle_id"):
            assert trip[field] != "", f"trip.{field} should be None, not ''"
    for prediction in predictions:
        assert prediction["stop_id"] != "", "stop_id should be None, not ''"


def test_departure_is_rarer_than_arrival(predictions):
    """~97% of predictions carry an arrival, ~2% a departure. Both optional."""
    arrivals = sum(1 for p in predictions if p["arrival_time"] is not None)
    departures = sum(1 for p in predictions if p["departure_time"] is not None)
    assert arrivals > departures
    assert arrivals > len(predictions) * 0.5


def test_failed_poll_decodes_to_nothing():
    assert decode_trip_updates(
        {"feed": "trip_updates", "fetched_at": FETCHED_AT, "status": 503}, b""
    ) == ([], [])
    assert decode_trip_updates(
        {"feed": "trip_updates", "fetched_at": FETCHED_AT, "status": None}, b""
    ) == ([], [])
