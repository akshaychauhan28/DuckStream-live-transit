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

## Redundancy — measured 2026-09-17 over 3 days of archive

725,633 decoded VehiclePositions records from 1,504 polls (2026-09-13 to 09-16,
polling every 30s):

| | |
|---|---|
| Unique readings | 308,528 |
| Duplicates | 417,105 (**57.5%**) |
| Distinct vehicles | 743 |

**How often a vehicle actually reports** (seconds between its own timestamp
updates, gaps over an hour excluded as out-of-service):

| min | p25 | median | p75 | p90 | max |
|---|---|---|---|---|---|
| 3 | 57 | 60 | 65 | 80 | 3,592 |

Only **5.5%** of updates arrive within 30s. So polling every 30s returned the
same reading roughly three times in seven — which is what moved the interval to
45s (DECISIONS.md #1).

Note that report interval and staleness are different measurements and are easy
to confuse: staleness (median 13s) is how old a reading is when fetched, while
report interval (median 60s) is how often a new one exists. Only the second
should set a polling rate.

**`(vehicle_id, vehicle_timestamp)` is safe as a dedupe key.** Across all
417,105 duplicates, every repeated key carried an identical position — zero
conflicts. A repeated timestamp never hid real movement.

Reproduce with `python scripts/measure_duplicates.py`.

## Most predictions are the timetable echoed back — measured 2026-09-17

Joining 131,713 captured predictions to the timetable in force gives a median
delay of **0 seconds** and puts 50.2% of buses within a minute of schedule.
That result is wrong, and it looks entirely reasonable.

Grouping predictions by how far ahead the stop is shows why:

| How far ahead | Rows | Exactly on time | Median delay |
|---|---|---|---|
| already at the stop | 4,040 | 2.9% | −31s |
| next 5 min | 14,887 | **4.0%** | +119s |
| 5 to 15 min | 28,527 | 15.2% | +85s |
| 15 to 30 min | 40,221 | 36.0% | +9s |
| 30 to 60 min | 35,058 | **56.6%** | 0s |
| over 60 min | 5,175 | 11.1% | −22s |

For a stop an hour away, **56.6% of predictions match the scheduled time to the
second.** Real buses are never exactly on time, so those rows are not
predictions at all — OC Transpo has nothing better to say yet and returns the
timetable unchanged. About **31% of all rows** are that.

Restricting to stops a bus is about to reach:

| | All predictions | Within 5 min of the stop |
|---|---|---|
| Median delay | 0s | **90s** |
| Within 60s | 50.2% | **26.9%** |
| Over 5 min late | 16.3% | 23.7% |

**Any punctuality figure must state which rows it used.** Counting every
prediction measures how closely OC Transpo's predictions track its own
timetable, which is not the same question as how late the buses are.

Reproduce with `python query/delay_check.py --polls 10`.

## Open questions

- **`start_time` is local service time**, not UTC. The sample shows
  `start_time: "16:56:00"` on a feed stamped 21:42 UTC — Ottawa is UTC-4 in
  August. GTFS also permits values past `24:00:00` for trips running after
  midnight. Both matter for the schema and for any date-based partitioning.
- **Is TripUpdates similarly redundant?** The same measurement has only been run
  on VehiclePositions. TripUpdates is ~89% of the bandwidth, so if its
  predictions also repeat between polls, 120s may still be faster than needed.
