# DuckStream

DuckStream is a data pipeline and web demo built around Ottawa's OC Transpo bus service. It collects live GTFS-Realtime data, preserves the original responses, turns them into an analytical dataset, and serves the results through a map and a natural-language question interface.

**[Open the live demo](https://duckstream-live-transit.onrender.com/)**

## Why this project exists

GTFS-Realtime feeds describe what is happening now. They do not provide a history of what buses did in the past. If a response is not collected when it is published, it is gone.

DuckStream creates that history itself. Its central design choice is to keep the original feed responses as an immutable archive. The decoded tables, arrival estimates, and demo database can all be rebuilt from that archive as the analysis improves.

## How it works

```text
OC Transpo live GTFS-Realtime feeds
        |
        v
Android phone running Termux
capture/capture.py -> capture/frames.py -> raw compressed archive
        |
        | laptop pulls files over the local network
        v
scripts/pull_from_phone.py
        |
        v
storage/writer.py + ingestion/decode.py
decoded and deduplicated daily Parquet tables
        |
        +--------------------------+
        |                          |
versioned static GTFS       processing/gtfs_time.py
        |                          |
        +------------+-------------+
                     v
             api/build_demo_db.py
             data/demo.duckdb
                     |
                     v
                api/main.py
         map · stop details · questions
```

### 1. Collect and preserve

`capture/capture.py` polls VehiclePositions and TripUpdates on separate configurable schedules. It writes the original response bytes and request metadata to hourly files. `capture/frames.py` stores each poll as a separate gzip member, so a file being written remains readable and a restart does not invalidate earlier complete frames. Failed polls are archived as well, which makes API failures distinguishable from times when collection was not running.

The collector currently runs on an Android phone in Termux. The phone serves its raw archive over the local network, and the laptop pulls new or changed files with `scripts/pull_from_phone.py`. Collection can continue while the laptop is asleep.

### 2. Decode and build the data lake

`ingestion/decode.py` parses the protobuf messages into records. `storage/writer.py` replays the raw archive one day at a time and writes compressed Parquet partitions for three distinct kinds of data: vehicle positions, trip state changes, and stop prediction changes.

The tables use different deduplication rules because their records mean different things. Repeated vehicle reports can be removed by vehicle and report timestamp. For predictions, only consecutive unchanged values are coalesced; a prediction that changes and later returns to an earlier value remains part of the history.

### 3. Join the timetable and estimate arrivals

The realtime feed contains predicted arrival times, not actual arrival times or schedule delays. `ingestion/static_gtfs.py` downloads and versions OC Transpo's static GTFS schedules. It keeps the original bundles so derived timetable tables can be recreated and observations can be matched to the schedule that fits the data.

`processing/gtfs_time.py` handles GTFS service times, including times past midnight, in Ottawa's timezone. `api/build_demo_db.py` uses it to join predictions to scheduled stop times and build a DuckDB database with route and stop names, coordinates, and delay values.

An arrival is an estimate: DuckStream uses the last prediction made shortly before a bus is expected at a stop. Stops without a usable close-in prediction are omitted. Cancelled trips and buses that disappear from the feed are not represented as arrivals, so the results describe observed arrivals rather than every scheduled trip or overall service reliability. Predictions far ahead can simply repeat the timetable, so they are not treated as measured arrivals.

### 4. Explore and ask questions

The FastAPI app in `api/main.py` serves the web page and its data endpoints:

- `/` serves the map and question interface.
- `/health` reports service and database health.
- `/stops` returns stops and summary values for the map.
- `/stop/{stop_id}` returns a stop's arrival summary by hour and route.
- `/ask` turns a plain-English question into a database query.

For `/ask`, `api/nl_to_sql.py` sends the database schema and its interpretation notes to the configured Groq model. The generated SQL is treated as untrusted: `api/guardrails.py` limits it to a single `SELECT`, disables filesystem access, enforces a row limit, and stops queries that run too long. The interface shows the SQL so an answer can be checked. The map and stop pages work without a Groq key; the question box requires one.

## Project structure

| Path | Purpose |
|---|---|
| `capture/` | Realtime collection, raw frame format, archive inspection, and Termux setup. |
| `ingestion/` | GTFS-Realtime protobuf decoding and versioned static GTFS downloads. |
| `processing/` | GTFS service-time parsing and timezone conversion. |
| `storage/` | Rebuild raw capture into daily Parquet tables. |
| `query/` | Delay analysis and an unfinished SQL sketch for gap detection. |
| `api/` | Arrival database builder, FastAPI service, web page, text-to-SQL, and SQL guardrails. |
| `scripts/` | Phone archive pull, data-quality measurements, model comparisons, and claim verification. |
| `docs/` | Feed analysis and design decisions, including documented reversals. |
| `tests/` | Tests for capture, framing, decoding, timetable selection, time handling, API behavior, and SQL restrictions. |
| `data/` | Local GTFS and Parquet outputs; the prepared `demo.duckdb` is used by the web service. |
| `raw/` | Locally pulled immutable capture archive. |

## Tools and technologies

- **Python** runs the collector, processing scripts, API, and data checks.
- **Requests** fetches the realtime feeds and communicates with the Groq API.
- **GTFS-Realtime protobuf bindings** decode OC Transpo's binary feed messages.
- **Polars and PyArrow** create and write Parquet tables.
- **DuckDB** joins the data, builds the demo database, and answers analytical queries.
- **FastAPI, Uvicorn, and Pydantic** provide the web API and request handling.
- **Leaflet** renders the interactive map in `api/page.html`.
- **Termux** runs the collector on Android; Python's HTTP server shares the raw files on the local network.
- **Render** hosts the public demo. Its deployment configuration is in `render.yaml`.
- **pytest** runs the project test suite.

The project does not use a message broker. The raw archive provides durability and replay; daily rebuilds provide repeatable processing.

## Run locally

Python 3.10 or newer is required. From the repository root:

```bash
python -m venv .venv
```

Activate the environment and install the project dependencies:

```bash
# Windows PowerShell
.venv\Scripts\Activate.ps1

# macOS / Linux
source .venv/bin/activate

python -m pip install -r requirements.txt
```

Copy `.env.example` to `.env` and set `OC_TRANSPO_PRIMARY_KEY`. The optional `CAPTURE_INTERVAL_*` variables control polling, and `CAPTURE_OUTPUT_DIR` controls where raw frames are written. Run the smoke test to check the key and feed responses:

```bash
python capture/smoke_test.py
```

To collect locally and rebuild the analytical data:

```bash
python capture/capture.py
python capture/inspect_archive.py

python ingestion/static_gtfs.py
python storage/writer.py
python api/build_demo_db.py
```

To serve the demo locally, set `GROQ_API_KEY` and `GROQ_MODEL` in `.env` if you want to use the question box, then run:

```bash
python -m uvicorn api.main:app --reload
```

The service reads `data/demo.duckdb` by default. Build it with `api/build_demo_db.py` if it is not present or needs refreshing.

## Run the tests

```bash
python -m pytest
```

## Capture deployment

The active collector setup is an Android phone running Termux. The phone collects the raw feeds and serves the archive on the local network; the laptop downloads it when available. See [`capture/DEPLOY.md`](capture/DEPLOY.md) for setup and operating instructions.

The public demo is deployed separately as a Render web service. It serves a prebuilt DuckDB file and does not run the collector. Set `GROQ_API_KEY` and `GROQ_MODEL` in the Render environment to enable natural-language questions.

## Design notes

- The raw archive is the source of truth; downstream data is rebuilt from it.
- Capture stores original feed bytes before decoding or analysis.
- GTFS time arithmetic preserves service-day meaning for trips scheduled after midnight.
- Timetable selection is validated against observed trip IDs because a plausible join can still use the wrong schedule.
- Arrival and delay values are explicitly estimates derived from predictions and static schedules.
- Generated SQL is checked and restricted before it can run against the demo database.

See [`docs/DECISIONS.md`](docs/DECISIONS.md) for the reasoning behind design choices and [`docs/FEED_NOTES.md`](docs/FEED_NOTES.md) for details about the realtime feed.
