# DuckStream

A data pipeline that collects its own dataset. It polls Ottawa's live OC Transpo
GTFS-Realtime feed, keeps every response, and turns the archive into a Parquet
lake you can ask questions of in plain English.

**Live demo: https://duckstream-live-transit.onrender.com** — a map of every
stop coloured by how late buses actually are there, and a question box.

The free instance sleeps after 15 minutes of no traffic, so the first request
takes about 30 seconds to wake it.

## What it found

The interesting part is not the pipeline, it is what came out of it.

**Buses are later than the published figures suggest.** Across 585,201 measured
arrivals: the median bus arrives **167 seconds late**, only **23%** get within a
minute of schedule, and **29%** are more than five minutes late. Between 4 and
6 pm the median reaches **225 seconds**.

**About 31% of the feed's "predictions" are the timetable handed back
unchanged.** For a stop an hour ahead, 56.6% of predicted arrival times match
the scheduled time *to the second*. Real buses are never exactly on time, so
those rows are not predictions — OC Transpo has nothing better to say yet.
Including them reports the median delay as **0 seconds**, which looks entirely
reasonable and is wrong. Restricting to buses within five minutes of the stop
moves it to **90 seconds**. Any punctuality number from this feed has to say
which rows it used. [Full working](docs/FEED_NOTES.md).

**Polling faster does not get you more data.** Vehicles report every 60 seconds
at the median, and only 5.5% of updates arrive within 30 seconds of the last
one. Polling every 30s returned the same reading roughly three times in seven.
The interval is 45s because that was measured, not guessed
([DECISIONS #1](docs/DECISIONS.md)).

**Timetable versions cannot be trusted to say when they apply.** Selecting the
timetable by its own advertised validity dates silently matched only **31% of
observed trips** — a join that returns rows and quietly answers a different
question. Selecting it by measured trip-id overlap instead restores 97–99%
coverage ([DECISIONS #10](docs/DECISIONS.md)).

## Why it exists

GTFS-Realtime is a snapshot feed. It tells you where the buses are right now,
and there is no endpoint that returns last Tuesday. So nobody keeps the
history, and "how late is this route, usually?" is a question no app can answer
— not because it is hard, but because the data is thrown away as it arrives.

Most data engineering demos run on a dataset that was already finished before
the project started. This one had to be collected first, day by day, which is
where all the interesting problems came from.

## Architecture

```
Android phone, Termux                Laptop
---------------------                ------
capture/capture.py                   scripts/pull_from_phone.py
  |  every 45s (vehicles)              |
  |  every 120s (trip updates)         v
  |  fetch -> gzip -> append          storage/writer.py
  v                                    |  decode, deduplicate, partition by day
raw/capture_<hour>-<run>.frames.gz     v
  ^                                  data/parquet/<table>/dt=<date>/part.parquet
  |                                    |
  immutable, never edited               v
  everything downstream                api/build_demo_db.py
  replays from here                     |  join versioned timetables -> arrivals
                                        v
                                      data/demo.duckdb
                                        |
                                        v
                                      api/main.py  (FastAPI on Render)
                                      map + per-stop pages + text-to-SQL
```

The split is the design: the collector does exactly one job that must never
fail, and everything experimental sits downstream of an immutable archive. A bug
in the transform layer costs a replay, not data — which happened, more than
once.

Three things fall out of that:

- **The archive survives being killed mid-write.** Each frame is its own gzip
  member, so a half-written file reads back as every complete frame it holds.
  ([DECISIONS #4](docs/DECISIONS.md))
- **Rebuild, don't append.** `writer.py` rewrites whole days from raw rather
  than appending. Slower, and it means the output always matches the current
  code instead of being a sediment of every version that ever ran.
- **Three tables, because there are three grains**: one row per vehicle report,
  one per trip state change, one per predicted arrival change.

## What has been collected

Every number here is recomputed from the data by `scripts/verify_claims.py`.

| | |
|---|---|
| Collecting since | 2026-09-13, continuously |
| Raw archive | 294 files, 720 MB |
| Poll coverage | 99.9% over the 10.4-day unattended window, 0.45% failed polls, longest gap 126s |
| Records decoded | 83,112,998 |
| Rows kept after deduplication | 50,818,969 (39% removed) |
| Parquet lake | 200 MB, partitioned by day |
| Measured arrivals | 585,201 across 5,544 stops and 177 routes |
| Tests | 128 |

**585,201 arrivals cover 8 days, not 15.** The timetable covering
2026-09-18 to 09-24 was withdrawn before it was fetched, and delay cannot be
derived without it. The raw capture for those days is intact, so they come back
whenever that timetable is recovered from an archive mirror. This is the reason
timetables are now selected by measurement and re-checked every six hours.

## Layout

| Path | What it is |
|---|---|
| `capture/` | The collector. Minimal by design — Python 3 and `requests`, nothing else |
| `ingestion/` | Protobuf decoding, and static GTFS download with version selection |
| `processing/` | GTFS time arithmetic (service days, times past 24:00:00) |
| `storage/` | `writer.py` — raw frames to partitioned Parquet |
| `query/` | Delay derivation and archive analysis |
| `api/` | FastAPI demo: text-to-SQL, execution guardrail, map, per-stop pages |
| `scripts/` | Measurement scripts. Every published number is reproducible from one |
| `docs/` | [DECISIONS.md](docs/DECISIONS.md) — why, including where it was wrong. [FEED_NOTES.md](docs/FEED_NOTES.md) — what the feed actually contains |

## Running it

**Requires Python 3.10+** for `X | Y` annotations. This matters mainly for the
collector, which runs wherever you put it.

```bash
python -m venv .venv
.venv\Scripts\activate           # Windows
pip install -r requirements.txt

copy .env.example .env           # then add your OC Transpo key
python capture/smoke_test.py     # two API calls: is the key good?
```

Then collect, and build:

```bash
python capture/capture.py        # runs until stopped
python capture/inspect_archive.py    # how healthy is the archive?

python ingestion/static_gtfs.py  # download the timetable
python storage/writer.py         # raw -> Parquet, all days
python api/build_demo_db.py      # Parquet + timetable -> demo.duckdb
```

Serve the demo locally. Text-to-SQL needs a `GROQ_API_KEY`; the map and the stop
pages work without one:

```bash
.venv\Scripts\python.exe -m uvicorn api.main:app --reload
```

See [capture/DEPLOY.md](capture/DEPLOY.md) for running the collector unattended.

## Honesty notes

Kept here on purpose, because the claims above should be checkable.

- **Collection is real-time** — 45-second polling of a live feed. Processing is
  batched: the laptop pulls the archive periodically and rebuilds, rather than
  streaming continuously.
- **There is no broker.** Redpanda was in the original plan and was cut without
  being built. The raw archive already provides durability and replay, which
  left the broker fan-out to a single consumer that does not exist. The full
  reasoning, including the bad reason it was planned in the first place, is in
  [DECISIONS #6](docs/DECISIONS.md).
- **Uptime is measured, not assumed.** `scripts/verify_claims.py` recomputes
  poll coverage, failure rate and longest gap from the archive itself, and
  distinguishes "we polled and the API failed" from "we were not running".
- **The arrival time is a proxy.** The feed never reports actual arrivals, so
  each row uses the last prediction made within 120 seconds of the bus reaching
  the stop. Cancelled trips and buses that vanished from the feed are absent, so
  this measures arrivals that happened rather than overall reliability.
- **The SQL for a question is written by a language model**, then checked before
  it runs: one `SELECT` only, a read-only connection with external file access
  disabled, a row limit and a 10-second watchdog. The generated SQL is shown
  with every answer, including when it is refused.
- **Any synthetic benchmark is labelled synthetic**, every time.

## Still open

- `query/gap_check.sql` is a stub. Coverage is computed in
  `scripts/verify_claims.py`; the SQL version was never written.
- The partitioning bake-off in [DECISIONS #7](docs/DECISIONS.md) was never run.
  Day partitioning was chosen on reasoning alone, which is exactly the kind of
  unmeasured choice this project otherwise avoids.
- Capture runs on an Android phone in Termux; see
  [capture/DEPLOY.md](capture/DEPLOY.md) and [DECISIONS #12](docs/DECISIONS.md).
