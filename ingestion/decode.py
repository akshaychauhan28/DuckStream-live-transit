"""
Decode raw VehiclePositions frames into records.

Not implemented — Phase 1.

Input is one frame as stored by capture/frames.py: the metadata dict plus the
raw protobuf bytes exactly as OC Transpo returned them. Output is one record
per vehicle in that response.

CONTRACT

    decode_vehicle_positions(meta: dict, body: bytes) -> list[dict]

    meta["feed"]        "vehicle_positions"
    meta["fetched_at"]  ISO 8601 UTC string — when we polled
    meta["status"]      HTTP status
    body                protobuf bytes, or b"" when the poll failed

RECORD SCHEMA — one per vehicle entity

    vehicle_id             str          entity.vehicle.vehicle.id
    fetched_at             int          epoch seconds, from meta["fetched_at"]
    feed_timestamp         int          epoch seconds, feed.header.timestamp
    vehicle_timestamp      int          epoch seconds, entity.vehicle.timestamp
    latitude               float
    longitude              float
    bearing                float | None
    speed                  float | None
    trip_id                str | None
    route_id               str | None
    start_time             str | None   local service time, may exceed 24:00:00
    start_date             str | None   YYYYMMDD
    schedule_relationship  int | None   keep the integer, not the name
    in_service             bool         True when the entity carries a trip

RULES

  1. A failed poll decodes to nothing. If meta["status"] is not 200, or the
     body is empty, return an empty list. Those frames are stored on purpose
     for gap analysis, but they hold no vehicles.

  2. Empty strings become None. Protobuf returns "" for absent string fields,
     which is a default, not a value. NULL is what makes `IS NULL` work in SQL
     and what Parquet stores efficiently.

  3. Optional numbers use HasField. `bearing` and `speed` are optional in the
     GTFS-RT spec. A missing speed and a speed of 0.0 are different facts: one
     means unknown, the other means stopped.

  4. in_service is True only when the entity actually carries a trip with a
     trip_id. Roughly 18% of vehicles have a position but no trip — they are
     kept deliberately, so that fleet counts stay correct and deadheading stays
     visible. This flag exists so that filtering them is explicit at query time
     rather than half-remembered.

  5. Both timestamps are kept. vehicle_timestamp is event time (when the bus
     reported) and is what analysis should use. fetched_at is observation time
     (when we polled) and is what proves uptime. Median gap 13s, max 637s.

NOTES

    Decoding:  from google.transit import gtfs_realtime_pb2
               feed = gtfs_realtime_pb2.FeedMessage()
               feed.ParseFromString(body)
               for entity in feed.entity: ...

    An entity carrying vehicle data has entity.HasField("vehicle").
    Parsing the ISO timestamp: datetime.fromisoformat(meta["fetched_at"])

Run the tests to check your work:

    .venv\\Scripts\\python.exe -m pytest tests/test_decode.py -v
"""

from datetime import datetime

from google.transit import gtfs_realtime_pb2


def decode_vehicle_positions(meta: dict, body: bytes) -> list[dict]:
    """Turn one VehiclePositions frame into a list of records. See module docstring."""
    # Failed polls are stored on purpose — they prove we were running when the
    # API wasn't — but they contain no vehicles.
    if meta.get("status") != 200 or not body:
        return []

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(body)

    # Both are the same for every record in this response: one is when we
    # asked, the other is when OC Transpo built the response.
    fetched_at = int(datetime.fromisoformat(meta["fetched_at"]).timestamp())
    feed_timestamp = feed.header.timestamp

    records = []
    for entity in feed.entity:
        # A feed can carry other entity types; skip anything that isn't a vehicle.
        if not entity.HasField("vehicle"):
            continue

        v = entity.vehicle        # the VehiclePosition
        p = v.position            # latitude, longitude, bearing, speed
        t = v.trip                # trip_id, route_id, start_time, start_date

        # ~18% of vehicles have a position but no trip: deadheading, between
        # trips, or out of service. They are kept, because dropping them would
        # quietly distort every fleet count. This flag makes filtering them a
        # deliberate choice at query time.
        in_service = v.HasField("trip") and bool(t.trip_id)

        records.append({
            "vehicle_id": v.vehicle.id,
            "fetched_at": fetched_at,
            "feed_timestamp": feed_timestamp,
            "vehicle_timestamp": v.timestamp,
            "latitude": p.latitude,
            "longitude": p.longitude,
            # bearing and speed are optional in the spec. HasField asks whether
            # the field was sent at all, which is not the same question as
            # whether it is zero — a stopped bus reports speed 0.0, and that is
            # a fact, not a missing value.
            "bearing": p.bearing if p.HasField("bearing") else None,
            "speed": p.speed if p.HasField("speed") else None,
            # Protobuf returns "" for absent strings. That is a default, not a
            # value, so it becomes None: SQL can then use IS NULL, and Parquet
            # stores it as a null rather than an empty string.
            "trip_id": t.trip_id or None,
            "route_id": t.route_id or None,
            "start_time": t.start_time or None,
            "start_date": t.start_date or None,
            # The same `or None` trick would be wrong here: 0 is a real value
            # (SCHEDULED), and `0 or None` is None, which would erase every
            # normally scheduled trip.
            "schedule_relationship": t.schedule_relationship if in_service else None,
            "in_service": in_service,
        })

    return records