"""
Turn the raw archive into queryable Parquet.

    python storage/writer.py                    # rebuild every day
    python storage/writer.py --date 2026-09-18  # one day only
    python storage/writer.py --list             # what has been built

Raw frames are gzipped protobuf: exactly what OC Transpo sent, and impossible
to query. Decoding one hour costs about a second, so every question asked
straight against raw pays that again for the whole archive. Parquet is read by
DuckDB directly, in SQL, without decoding anything.

REBUILD, DON'T APPEND

Each run rewrites whole days from raw. That is slower than appending, and it is
the point: the transforms keep changing, and a rebuild means the output always
matches the current code rather than being a sediment of every version that has
ever run. Raw never changes, so this is always safe to redo.

THREE TABLES, BECAUSE THERE ARE THREE GRAINS

    vehicle_positions   one row per vehicle report
    trips               one row per trip, per change in its state
    predictions         one row per stop, per change in its predicted arrival

DEDUPLICATION

Vehicle positions repeat because we poll faster than buses report. Measured at
57.5% duplicates, and (vehicle_id, vehicle_timestamp) was verified safe across
417,105 of them: every repeat carried an identical position.

Predictions are different. They are revised as a bus falls behind, 9 times for
a typical stop, and they can return to a value they held before. So only
*consecutive* repeats are dropped, and each row records the window it was valid
for:

    trip     stop  arrival   first_seen  last_seen
    6432100  87    13:07:00  11:44       12:14      <- 16 polls, one row
    6432100  87    18:24:32  12:16       12:16

That halves the rows while keeping the sequence reconstructable, including the
oscillations. It also makes "the last prediction before the bus arrived" — the
best available proxy for when it actually arrived — a single query.

DAY BOUNDARIES

Days are processed one at a time so memory stays bounded. A prediction sequence
running across midnight restarts its deduplication, which produces at most one
extra row per stop per boundary. Cheap, and it keeps each day independently
rebuildable.
"""

import argparse
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
for sub in ("capture", "ingestion"):
    sys.path.insert(0, str(ROOT / sub))

import polars as pl  # noqa: E402

from decode import decode_trip_updates, decode_vehicle_positions  # noqa: E402
from frames import read_frames  # noqa: E402

RAW = Path(ROOT / "raw")
OUT = Path(ROOT / "data" / "parquet")

# Pinned rather than inferred. Schema drift across days is what turns a lake
# into a pile of files that will not read together.
VEHICLE_SCHEMA = {
    "vehicle_id": pl.Utf8,
    "fetched_at": pl.Int64,
    "feed_timestamp": pl.Int64,
    "vehicle_timestamp": pl.Int64,
    "latitude": pl.Float64,
    "longitude": pl.Float64,
    "bearing": pl.Float64,
    "speed": pl.Float64,
    "trip_id": pl.Utf8,
    "route_id": pl.Utf8,
    "start_time": pl.Utf8,
    "start_date": pl.Utf8,
    "schedule_relationship": pl.Int32,
    "in_service": pl.Boolean,
}

TRIP_SCHEMA = {
    "trip_id": pl.Utf8,
    "start_date": pl.Utf8,
    "start_time": pl.Utf8,
    "route_id": pl.Utf8,
    "schedule_relationship": pl.Int32,
    "vehicle_id": pl.Utf8,
    "trip_update_timestamp": pl.Int64,
    "prediction_count": pl.Int32,
    "first_seen": pl.Int64,
    "last_seen": pl.Int64,
}

PREDICTION_SCHEMA = {
    "trip_id": pl.Utf8,
    "start_date": pl.Utf8,
    "stop_sequence": pl.Int32,
    "stop_id": pl.Utf8,
    "arrival_time": pl.Int64,
    "departure_time": pl.Int64,
    "schedule_relationship": pl.Int32,
    "first_seen": pl.Int64,
    "last_seen": pl.Int64,
}


def day_of(path: Path) -> str:
    """capture_2026-09-18T17-215824-59c5.frames.gz -> 2026-09-18"""
    return path.name.split("_", 1)[1][:10]


def files_by_day() -> dict[str, list[Path]]:
    grouped = defaultdict(list)
    for path in sorted(RAW.glob("capture_*.gz")):
        grouped[day_of(path)].append(path)
    return dict(grouped)


def build_day(day: str, paths: list[Path]) -> dict:
    """Decode one day of raw frames into three deduplicated tables."""
    vehicles = []
    seen_positions = set()

    trip_state = {}        # (trip_id, start_date) -> [values, first_seen, last_seen]
    trips_out = []

    pred_state = {}        # (trip_id, start_date, stop_sequence) -> as above
    preds_out = []

    frames = 0
    raw_rows = 0

    for path in paths:
        for meta, body in read_frames(path):
            if meta.get("status") != 200:
                continue
            frames += 1

            if meta["feed"] == "vehicle_positions":
                for row in decode_vehicle_positions(meta, body):
                    raw_rows += 1
                    # A position never changes once reported, so an exact repeat
                    # carries nothing. Verified safe across 417,105 duplicates.
                    key = (row["vehicle_id"], row["vehicle_timestamp"])
                    if key in seen_positions:
                        continue
                    seen_positions.add(key)
                    vehicles.append(row)
                continue

            trips, predictions = decode_trip_updates(meta, body)

            for trip in trips:
                raw_rows += 1
                _accumulate(
                    trip_state, trips_out,
                    key=(trip["trip_id"], trip["start_date"]),
                    values={
                        "schedule_relationship": trip["schedule_relationship"],
                        "vehicle_id": trip["vehicle_id"],
                        "trip_update_timestamp": trip["trip_update_timestamp"],
                        "prediction_count": trip["prediction_count"],
                    },
                    static={
                        "trip_id": trip["trip_id"], "start_date": trip["start_date"],
                        "start_time": trip["start_time"], "route_id": trip["route_id"],
                    },
                    when=trip["fetched_at"],
                )

            for p in predictions:
                raw_rows += 1
                _accumulate(
                    pred_state, preds_out,
                    key=(p["trip_id"], p["start_date"], p["stop_sequence"]),
                    values={
                        "arrival_time": p["arrival_time"],
                        "departure_time": p["departure_time"],
                        "schedule_relationship": p["schedule_relationship"],
                    },
                    static={
                        "trip_id": p["trip_id"], "start_date": p["start_date"],
                        "stop_sequence": p["stop_sequence"], "stop_id": p["stop_id"],
                    },
                    when=p["fetched_at"],
                )

    # Whatever is still open at the end of the day is closed and emitted.
    _flush(trip_state, trips_out)
    _flush(pred_state, preds_out)

    written = {}
    for table, rows, schema in (
        ("vehicle_positions", vehicles, VEHICLE_SCHEMA),
        ("trips", trips_out, TRIP_SCHEMA),
        ("predictions", preds_out, PREDICTION_SCHEMA),
    ):
        written[table] = _write(table, day, rows, schema)

    return {"day": day, "files": len(paths), "frames": frames,
            "raw_rows": raw_rows, "tables": written}


def _accumulate(state, out, key, values, static, when):
    """
    Drop consecutive repeats, keeping the window each value was valid for.

    Only consecutive ones. A prediction that returns to a value it held earlier
    is a real change and gets its own row, so the sequence — oscillations and
    all — stays reconstructable.
    """
    current = state.get(key)
    if current is not None and current["values"] == values:
        current["last_seen"] = when          # still valid, extend the window
        return
    if current is not None:
        out.append(_row(current))
    state[key] = {"values": values, "static": static,
                  "first_seen": when, "last_seen": when}


def _row(entry: dict) -> dict:
    return {**entry["static"], **entry["values"],
            "first_seen": entry["first_seen"], "last_seen": entry["last_seen"]}


def _flush(state, out):
    for entry in state.values():
        out.append(_row(entry))
    state.clear()


def _write(table: str, day: str, rows: list[dict], schema: dict) -> dict:
    target = OUT / table / f"dt={day}"
    target.mkdir(parents=True, exist_ok=True)
    path = target / "part.parquet"

    frame = pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)
    frame.write_parquet(path, compression="zstd", statistics=True)
    return {"rows": len(frame), "bytes": path.stat().st_size,
            "path": str(path.relative_to(ROOT)).replace("\\", "/")}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--date", help="rebuild one day, YYYY-MM-DD")
    parser.add_argument("--list", action="store_true", help="show what is built")
    args = parser.parse_args()

    if args.list:
        if not OUT.exists():
            print("Nothing built yet.")
            return 0
        for table in sorted(p.name for p in OUT.iterdir() if p.is_dir()):
            parts = sorted((OUT / table).glob("dt=*/part.parquet"))
            total = sum(p.stat().st_size for p in parts)
            rows = sum(pl.scan_parquet(p).select(pl.len()).collect().item() for p in parts)
            print(f"{table:20s} {len(parts):>3} days  {rows:>12,} rows  {total/1e6:>8,.1f} MB")
        return 0

    grouped = files_by_day()
    if args.date:
        grouped = {args.date: grouped.get(args.date, [])}
        if not grouped[args.date]:
            print(f"No raw files for {args.date}")
            return 1

    print(f"{'day':<12}{'files':>6}{'frames':>8}{'raw rows':>12}"
          f"{'vehicles':>11}{'trips':>9}{'predictions':>13}{'MB':>8}{'secs':>7}")
    for day, paths in sorted(grouped.items()):
        started = time.perf_counter()
        result = build_day(day, paths)
        tables = result["tables"]
        size = sum(t["bytes"] for t in tables.values()) / 1e6
        print(f"{day:<12}{result['files']:>6}{result['frames']:>8}"
              f"{result['raw_rows']:>12,}"
              f"{tables['vehicle_positions']['rows']:>11,}"
              f"{tables['trips']['rows']:>9,}"
              f"{tables['predictions']['rows']:>13,}"
              f"{size:>8,.1f}{time.perf_counter() - started:>7.1f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
