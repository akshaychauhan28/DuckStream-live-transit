# What the OC Transpo feed actually contains

Measured from real payloads captured **2026-08-20 21:42 UTC** (a weekday
evening, ~17:42 local). Fleet size varies by time of day, so percentages here
are a snapshot, not a constant — but field *presence* is structural and should
hold.

Reproduce with `python capture/smoke_test.py`, then analyse the fixtures.

## VehiclePositions — 503 entities, 38 KB raw / 12.5 KB gzipped

| Field | Coverage | Notes |
|---|---|---|
| `position.latitude` / `longitude` | 100% | |
| `position.bearing` | 100% | |
| `position.speed` | 100% | 278/503 non-zero; range 0.45–28.16, median 7.82 |
| `vehicle.timestamp` | 100% | when the *vehicle* reported, not when we polled |
| `vehicle.vehicle.id` | 100% | the stable vehicle identifier |
| `trip.trip_id` / `route_id` / `start_time` / `start_date` | **81.7%** | see below |

**18.3% of vehicles carry no trip or route.** 92 of 503 had `route_id = ""`.
These are buses with a valid GPS position but no trip assignment — deadheading,
between trips, or out of service. Any delay-by-route aggregation has to decide
what to do with them explicitly, or it silently drops a fifth of the fleet.

**Staleness** (`feed.header.timestamp - vehicle.timestamp`), in seconds:

| min | p25 | median | p75 | p90 | max |
|---|---|---|---|---|---|
| 7 | 8 | 13 | 18 | 21 | 637 |

Only 8 of 503 exceeded 60s, and 4 exceeded 300s. So the feed is fresh for the
overwhelming majority, with a small stale tail. Both timestamps still need to be
carried through the pipeline — event time and observation time are different
facts — but the tail is the exception, not the norm.

`schedule_relationship` on the trip: 498 × `0` (SCHEDULED), 5 × `8` (NEW).

## TripUpdates — 676 entities, 331 KB raw / 106 KB gzipped

| Field | Coverage |
|---|---|
| `trip.trip_id` / `route_id` / `start_time` / `start_date` | 100% |
| `trip_update.timestamp` | 100% |
| `trip_update.stop_time_update` | 95.6% (646/676) |
| `trip_update.vehicle` | 92.0% |

14,645 stop_time_updates total; median 21 per trip, max 75.

| Sub-field | Coverage of 14,645 |
|---|---|
| `stop_sequence`, `stop_id`, `schedule_relationship` | ~100% |
| `arrival` | 96.9% (14,189) |
| `departure` | **2.1%** (314) |

Of the arrivals that exist, **100% carry `arrival.time` and 0% carry
`arrival.delay`.** Zero explicit delay values across the entire payload.

`stop_time_update.schedule_relationship`: 14,503 × `0` (SCHEDULED),
142 × `1` (SKIPPED) — real skipped-stop signal.

Trip-level `schedule_relationship`: 637 × `0` (SCHEDULED),
**30 × `3` (CANCELED)**, **9 × `8` (NEW)**.

`NEW` is defined in the spec as *"an extra trip unrelated to any existing trips,
for example, to respond to sudden passenger load"* — OC Transpo dispatching
extra buses against demand, visible in the feed.

## The consequence: delay must be derived

The feed gives predicted **absolute arrival times**, never delays. So:

```
delay = predicted_arrival_time  -  scheduled_arrival_time
                (feed)                (static GTFS stop_times.txt)
```

There is no path to a delay number that does not go through static GTFS. This
moves static GTFS from "important context" to a hard prerequisite for the
project's central analysis. See DECISIONS.md #10.

### Not every trip can have a delay

The formula needs a scheduled counterpart in static GTFS, and several
`schedule_relationship` values mean no such counterpart exists:

| Value | Meaning | Has a scheduled time? |
|---|---|---|
| `0` SCHEDULED | running against its GTFS schedule | yes |
| `1` ADDED | deprecated in the spec | no |
| `2` UNSCHEDULED | frequency-based, `exact_times=0` | no |
| `3` CANCELED | was scheduled, removed | scheduled, but never arrives |
| `6` DUPLICATED | copy of a trip at a different time | not directly |
| `7` DELETED | removed, must not be shown to users | exclude entirely |
| `8` NEW | extra trip added for demand | **no** |

In the sample, 30 CANCELED + 9 NEW = **39 of 676 trips (5.8%)** cannot produce a
normal delay figure. They are not bad data — they're real operational events —
but a delay aggregation has to exclude them deliberately rather than let the
join drop them silently.

Enum definitions are in
[gtfs-realtime.proto](https://github.com/google/transit/blob/master/gtfs-realtime/proto/gtfs-realtime.proto),
which is the source of truth for both feeds.

## Open questions

- **`start_time` is local service time**, not UTC. The sample shows
  `start_time: "16:56:00"` on a feed stamped 21:42 UTC — Ottawa is UTC-4 in
  August. GTFS also permits values past `24:00:00` for trips running after
  midnight. Both matter for the schema and for any date-based partitioning.
- **Dedupe rate is not yet measurable.** It needs two consecutive polls to
  compute, and we only captured one snapshot. Measure it on the first hour of
  real capture — it directly sizes the Parquet layer.
