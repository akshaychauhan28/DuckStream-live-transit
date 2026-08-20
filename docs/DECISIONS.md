# Design Decisions

Every entry records what was decided, what the alternatives were, and what it
costs, so the reasoning stays recoverable later.

Entries marked **OPEN** are unresolved and need a call before the phase they
block.

---

## 1. Polling interval vs. API quota — **OPEN, blocks continuous capture**

**Decision:** default to 30-second polling, but treat it as unverified.

30s across both feeds is **5,760 calls/day**, ~173,000/month. OC Transpo serves
GTFS-RT through Azure API Management, and APIM products almost always carry a
quota — some free tiers are as low as a few thousand calls *per week*, which
would make 30s polling impossible.

The quota is visible only behind the portal login, so it has to be read off the
Products page or off the response headers (`smoke_test.py` prints any
`RateLimit-*` / `Quota-*` headers the API returns — those are more trustworthy
than documentation).

**Cost if wrong:** hitting the cap mid-month silently kills collection, which is
the one failure this project cannot absorb. Verify before starting continuous
capture, and set `CAPTURE_INTERVAL_SECONDS` to fit the real quota.

**Note:** if the quota forces a slower interval, that is not a disaster —
vehicle positions typically refresh every 20–60s anyway, so 60s polling loses
less than it sounds like. Measure the real refresh rate from the fixtures before
assuming 30s is even useful.

---

## 2. Ingestion runs on an always-on VM, not the laptop

**Decision:** capture runs on a small always-free cloud VM.

GTFS-RT is a snapshot feed. There is no historical endpoint. Data not collected
at 06:00 on a Tuesday does not exist anywhere, ever. A laptop that sleeps
overnight collects zero rush-hour data — the most analytically valuable window
in the entire dataset — so the archive would have a hole exactly where the
interesting queries live.

**Alternative considered:** run on the laptop and accept gaps. Rejected: it
would quietly invalidate the project's central claim of a continuous archive.

**Cost:** an operational dependency (a VM to provision and monitor) that a
static dataset like NYC Taxi would not have. That is the real price of using
live data, and it's worth naming rather than pretending live data is free.

**Verify at signup:** cloud always-free terms have churned; read your own
tenancy's current limits rather than trusting older guides. Also check whether
idle-instance reclamation applies — a light poller may not clear the activity
threshold.

---

## 3. Raw file buffer on the VM, replayed locally — not a broker on the VM, not a tunnel

**Decision:** the VM writes raw gzipped response bytes to hourly files and does
nothing else. The laptop syncs those files down and replays them into local
Redpanda.

The problem this solves: the laptop is behind home-router NAT with a dynamic IP,
so a producer running in a datacenter cannot open a connection to a broker on
the desk.

**Alternatives considered:**

| Option | Why rejected |
|---|---|
| Redpanda on the VM | An always-free micro shape has ~1GB RAM. Redpanda expects real memory; an OOM kill at 2am destroys irreplaceable data, and the component that killed it was added for *learning*, not for load. Also still needs a file sync to the laptop, so it adds a failure mode without removing one. |
| Tunnel (Tailscale / Cloudflare Tunnel) to the laptop | Works, and is easy. But when the laptop sleeps the producer's buffer fills within ~30s and then blocks or drops. Closing the lid on a Friday costs the weekend. This makes the laptop a required participant in collection, which defeats the entire reason for decision #2. |

**What this buys:** the durable layer is a few dozen lines of Python with one
dependency, and everything experimental sits *downstream* of an immutable
archive. A bug in the transform layer costs a replay, not data.

**Cost, stated honestly:** the laptop sees data in batches, not continuously.
Collection is real-time; local processing is near-real-time. Since every
analysis runs over a historical archive anyway this costs nothing real — but
the claim should be phrased that way, not as end-to-end streaming.

**Its own failure modes:** the VM's disk fills (rotate + monitor free space), or
the VM dies (rsync often so the laptop holds a second copy).

---

## 4. Raw archive framing format

**Decision:** length-prefixed frames (JSON metadata block + raw response bytes)
inside hourly gzip files. See `capture/frames.py` for the full rationale.

Short version: one-file-per-poll creates ~5,760 tiny files/day and makes rsync
crawl; base64-in-JSONL is friendlier but defeats gzip on binary payloads and
costs meaningful space over 30 days.

Storing failed polls as frames (status, no body) is deliberate — it lets gap
analysis distinguish "the API returned 503" from "we weren't running", which are
different claims to make in a writeup.

---

## 5. No model name in source

**Decision:** `GROQ_MODEL` lives in `.env`, referenced once in `nl_to_sql.py`,
and never appears in a docstring or the README.

Model lineups move fast; a name hardcoded on day 0 will be stale by day 23.
Prove the abstraction by running the prompt against two different models during
Phase 4 — if that's a one-line change, the claim is demonstrated rather than
asserted.

---

## 6. Redpanda, despite the volume not requiring it

**Decision:** keep the broker. State plainly that it isn't load-necessary.

At ~30 msg/s with one consumer, a directory of files covers most of what a
broker provides — which is precisely what decision #3 does. Redpanda is here to
work with topics, partitions, consumer groups, and offsets directly rather than
in theory.

**The division of labour worth articulating:** raw gzip files provide
*durability and replay*; Redpanda provides *stream-processing semantics and
fan-out*. They cover what the other is bad at. Claiming the broker was needed
for throughput would be false and easy to catch.

---

## 7. Partitioning strategy — **OPEN, decided in Phase 2 by measurement**

**Decision deferred deliberately.** Build both date-only+sorted and date+route,
benchmark on real accumulated data, keep the winner, publish the numbers.

Prior expectation is that date+route loses (~120 routes × 30 days = 3,600+
directories of small files, which is slow on both HDD and DuckDB), but the
measurement is the artifact, so it should not be pre-empted here.

---

## 8. Python 3.14 locally, 3.10 as the floor

**Verified 2026-08-21:** polars, duckdb, gtfs-realtime-bindings, protobuf,
requests, confluent-kafka, fastapi, pydantic, and pyarrow all resolve wheels on
3.14. No downgrade needed.

**Minimum is 3.10**, set by `X | Y` union syntax in annotations. Relevant only
to the capture VM, whose Python version isn't ours to choose — Ubuntu 22.04
ships 3.10 and 24.04 ships 3.12, so either works.

`from __future__ import annotations` was removed from all files. It exists to
make modern annotation syntax work on old Python and to defer evaluation;
3.14 defers natively via PEP 649, and nothing here targets below 3.10. In the
stub files, which have no annotations at all, it was doing nothing. Removed on
the principle that every line in this repo should be defensible — including the
idiomatic ones.

Versions are unpinned during Phase 1; pin with `pip freeze` once the pipeline is
stable so the archive stays reproducible.

---

## 9. TripUpdates poll interval — **OPEN, decide before continuous capture**

Measured 2026-08-20: TripUpdates is ~106 KB gzipped per poll against
VehiclePositions' ~12.5 KB. It is **~89% of the archive by size**.

| Polling | Per day | 30 days |
|---|---|---|
| Both at 30s | ~341 MB | ~10.2 GB |
| Both at 60s | ~171 MB | ~5.1 GB |
| VP 30s + TU 120s | ~112 MB | ~3.4 GB |

The tradeoff: VehiclePositions at 30s is what gives trajectory resolution for
deriving speed and dwell time. TripUpdates carries predicted arrival times,
which do not change meaningfully within 30s — so slowing it is the cheap lever.

`capture.py` now supports per-feed intervals
(`CAPTURE_INTERVAL_TRIP_UPDATES_SECONDS`). Both default to 30s; nothing is
imposed. **Your call**, and worth making deliberately, because the archive
cannot be re-collected at a higher resolution later.

---

## 10. Delay must be derived — static GTFS is a hard dependency

**Measured, not assumed:** across 14,189 arrival predictions in a real
TripUpdates payload, **100% carry `arrival.time` and 0% carry `arrival.delay`.**

The feed reports *when a bus is predicted to arrive*, never *how late it is*.
So:

    delay = predicted_arrival_time (feed) - scheduled_arrival_time (static GTFS)

There is no route to a delay figure that avoids static GTFS. This changes its
status from "important, adds human-readable names" to **a prerequisite for the
project's central analysis**. Phase 2 should treat it as blocking, not
supporting.

It also makes the versioning problem load-bearing: joining September's
observations against October's schedule produces numbers that are wrong but
never error. See `ingestion/static_gtfs.py`.

Full field coverage in [FEED_NOTES.md](FEED_NOTES.md).
