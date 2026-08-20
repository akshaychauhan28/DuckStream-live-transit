# DuckStream

A local-first, real-time streaming analytics engine over live Ottawa (OC Transpo)
transit data. Ingests the GTFS-Realtime feed continuously, accumulates it as a
partitioned Parquet lake, and answers plain-English questions over it through an
AI query layer.

Built solo over 30 days, at near-zero cost, documented daily.

> **Status:** Phase 1 — ingestion. Nothing below Phase 1 is implemented yet.

## Why this exists

Most data engineering demos run on a static dataset that was already finished
before the project started. This one collects its own. GTFS-RT is a snapshot
feed with no historical endpoint, so the archive here is genuinely accumulated,
day by day — and at the end it's a queryable record of Ottawa transit telemetry
that doesn't exist anywhere else.

## Architecture

```
Always-on VM                    Laptop
------------                    ------
capture.py                      rsync raw files down
  |  every 30s                    |
  |  fetch, gzip, append          v
  v                             replay.py --> Redpanda --> Polars --> Parquet
raw/capture_<hour>.jsonl.gz       (decode)     (topic)   (transform)  (lake)
  ^                                                                     |
  |                                                                     v
  immutable. never edited.                                       DuckDB + FastAPI
  everything downstream                                          + LLM text-to-SQL
  replays from here                                              (public demo)
```

The split matters: the VM does exactly one job that must never fail, and
everything experimental sits downstream of an immutable raw archive. A bug in
the transform layer costs a replay, not data. Full rationale in
[docs/DECISIONS.md](docs/DECISIONS.md).

## Layout

| Path | What it is |
|---|---|
| `capture/` | Raw capture for the always-on VM. Minimal by design. |
| `ingestion/` | Decode + publish to Redpanda; static GTFS versioning; compose file |
| `processing/` | Polars streaming consumer |
| `storage/` | Parquet writer + partitioning bake-off |
| `query/` | DuckDB benchmarks and gap analysis |
| `api/` | FastAPI + text-to-SQL + execution guardrail |
| `docs/` | Spec and design decisions |

## Getting started

**Requires Python 3.10+.** The code uses `X | Y` union syntax in annotations,
which is native from 3.10 onward. This matters mainly for the capture VM —
check its Python version before deploying (Ubuntu 22.04 ships 3.10, 24.04 ships
3.12; both are fine).

```bash
python -m venv .venv
.venv\Scripts\activate          # Windows
pip install -r requirements.txt

copy .env.example .env          # then fill in your OC Transpo key
```

Verify the key and capture your first fixtures — two API calls:

```bash
python capture/smoke_test.py
```

Then start collecting. See [capture/DEPLOY.md](capture/DEPLOY.md) for running on
the laptop now and moving to the VM later.

## Honesty notes

Kept here deliberately, because the claims in this repo should be checkable:

- **Collection is real-time** (30s polling of a live feed). Local processing is
  batched, since the laptop syncs the archive periodically rather than
  streaming continuously.
- **Redpanda is not load-necessary** at this volume. It's here for hands-on
  Kafka semantics, and the raw file archive is what actually provides
  durability and replay. See DECISIONS.md #6.
- **Any synthetic benchmark is labelled as synthetic**, every time.
- **Uptime is measured, not assumed** — see `query/gap_check.sql`.
