"""
Replay: raw capture frames -> decoded records -> Redpanda topic.

This replaces the spec's separate poller.py + producer.py. Under the Option A
design the *fetching* happens on the VM (capture/capture.py), so what's left
here is decode-and-publish, which is one job.

    raw/capture_2026-08-21T14.jsonl.gz  ->  topic: vehicle-positions

Because it reads from files rather than the live API, it is fully re-runnable.
That is the point: when the transform logic changes on day 19, you replay 19
days of archive through the fixed code instead of losing them.

------------------------------------------------------------------------------
Not implemented — Phase 1.
------------------------------------------------------------------------------

Contract:
    read_frames(path) yields (meta: dict, body: bytes)
      meta["feed"]        "vehicle_positions" | "trip_updates"
      meta["fetched_at"]  ISO8601 UTC, when WE made the request
      meta["status"]      HTTP status; skip frames where this isn't 200
      body                raw protobuf bytes, or b"" on failure

    Output: one Kafka message per vehicle entity, keyed by vehicle_id.

Design questions to resolve before writing:

  1. Keying. Keying by vehicle_id puts all pings for one vehicle in the same
     partition, which preserves per-vehicle ordering. Do you need that ordering?
     What breaks if a vehicle's pings arrive out of order?

  2. Two timestamps, and they are not the same thing.
     meta["fetched_at"] is when you polled.
     entity.vehicle.timestamp is when the VEHICLE last reported.
     These can differ by minutes. Which one is "the" event time, and which
     one does dedupe key on? Getting this wrong is the day-19 bug.

  3. Idempotency. If you replay the same file twice, does the downstream end up
     with duplicates? Where should that be prevented — here, or in the consumer?

  4. Failed frames. Frames with status != 200 are stored on purpose. Skip them
     here, but don't delete them — query/gap_check.sql needs them to tell
     "API was down" apart from "we weren't running".

Useful commands while developing:
    docker exec -it redpanda rpk topic create vehicle-positions
    docker exec -it redpanda rpk topic consume vehicle-positions --num 5
"""

# from google.transit import gtfs_realtime_pb2
# from confluent_kafka import Producer
# from capture.frames import read_frames


def main() -> int:
    raise NotImplementedError("Phase 1 — see module docstring.")


if __name__ == "__main__":
    raise SystemExit(main())
