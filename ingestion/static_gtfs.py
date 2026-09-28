"""
Download and version the static GTFS Schedule bundle.

    python ingestion/static_gtfs.py           # fetch if changed
    python ingestion/static_gtfs.py --list    # show what we hold

WHY THIS EXISTS

The realtime feed carries predicted arrival times and never a delay: 14,189
predictions in a real payload, zero delay values. Delay only exists as

    delay = predicted arrival (feed) - scheduled arrival (this bundle)

so every delay number in the project comes out of a join against these files.

WHY IT IS VERSIONED

OC Transpo publishes one bundle at a time and the URL serves only the current
one. The bundle held on 2026-09-17 covers 2026-09-10 to 2026-10-10; when it
rotates, the previous one is gone from the URL.

An observation from September has to be compared against September's timetable.
Use October's instead and the join still succeeds — it just returns numbers
that are wrong, with nothing to indicate it. That silent failure is why
bundle_for() refuses by default rather than guessing.

WHAT IS STORED

    data/gtfs/
      manifest.json                 every version we hold, and when
      <feed_version>_<sha8>/
        bundle.zip                  the original download, untouched
        trips.parquet               converted for querying
        stop_times.parquet
        ...

The zip is kept for the same reason the raw capture archive is kept: it is the
source of truth, and anything derived can be rebuilt from it.

shapes.txt is deliberately not converted. It is 36 MB per version and only
describes the lines drawn on a map, which no delay calculation needs. It stays
inside the zip if it is ever wanted.

Tables are converted to Parquet with every column left as text. GTFS times can
read "25:35:00" — 1:35am on the next service day — which is not a valid clock
time and breaks any automatic type detection. Converting types is a deliberate
step later, not something to let a CSV reader guess at.
"""

import argparse
import hashlib
import io
import json
import tempfile
import urllib.error
import urllib.request
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
GTFS_DIR = ROOT / "data" / "gtfs"
MANIFEST = GTFS_DIR / "manifest.json"
LOG = ROOT / "logs" / "gtfs.log"

URL = "https://oct-gtfs-emasagcnfmcgeham.z01.azurefd.net/public-access/GTFSExport.zip"
USER_AGENT = (
    "DuckStream/0.1 "
    "(+https://github.com/akshaychauhan28/DuckStream-live-transit)"
)

# Everything the delay join and human-readable names need. shapes.txt is left
# out on purpose — see the module docstring.
TABLES = [
    "agency",
    "routes",
    "trips",
    "stops",
    "stop_times",
    "calendar",
    "calendar_dates",
    "feed_info",
]


# --------------------------------------------------------------------------
# manifest
# --------------------------------------------------------------------------

def record(message: str) -> None:
    """Print, and append to a log. A scheduled run has no console to watch, and
    the whole point of scheduling this is to notice a republication."""
    print(message)
    try:
        LOG.parent.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        with LOG.open("a", encoding="utf-8") as fh:
            fh.write(f"{stamp}  {message}" + chr(10))
    except OSError:
        pass  # never let logging break the download


def load_manifest() -> dict:
    if MANIFEST.exists():
        return json.loads(MANIFEST.read_text(encoding="utf-8"))
    return {"versions": []}


def save_manifest(manifest: dict) -> None:
    GTFS_DIR.mkdir(parents=True, exist_ok=True)
    MANIFEST.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def _as_date(value) -> date:
    """Accept 'YYYYMMDD', 'YYYY-MM-DD', a date, or a datetime."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip().replace("-", "")
    return datetime.strptime(text, "%Y%m%d").date()


def bundle_for(day, manifest: dict | None = None, allow_approximate: bool = False) -> dict:
    """
    Return the bundle whose service dates cover `day`.

    Raises LookupError when nothing covers it. That is deliberate: joining an
    observation to the wrong timetable produces a delay figure that looks
    entirely normal and is simply wrong, and nothing downstream can detect it.

    allow_approximate=True falls back to the most recent bundle that started on
    or before `day`, and marks the result with approximate=True so the looseness
    travels with the data instead of living in somebody's memory.
    """
    manifest = manifest if manifest is not None else load_manifest()
    wanted = _as_date(day)

    covering = [
        v for v in manifest["versions"]
        if _as_date(v["feed_start_date"]) <= wanted <= _as_date(v["feed_end_date"])
    ]
    if covering:
        # More than one can cover a day around a changeover; the most recently
        # published one is the one that was actually in force.
        best = max(covering, key=lambda v: v["first_fetched_at"])
        return {**best, "approximate": False}

    if allow_approximate:
        earlier = [v for v in manifest["versions"]
                   if _as_date(v["feed_start_date"]) <= wanted]
        if earlier:
            best = max(earlier, key=lambda v: _as_date(v["feed_start_date"]))
            return {**best, "approximate": True}

    held = ", ".join(
        f"{v['feed_version']} ({v['feed_start_date']}-{v['feed_end_date']})"
        for v in manifest["versions"]
    ) or "none"
    raise LookupError(
        f"No GTFS bundle covers {wanted}. Held: {held}. "
        f"Fetch the right version, or pass allow_approximate=True to use the "
        f"nearest earlier bundle and accept that the numbers are approximate."
    )


# --------------------------------------------------------------------------
# download
# --------------------------------------------------------------------------

def _read_feed_info(zf: zipfile.ZipFile) -> dict:
    """feed_info.txt states the period this bundle describes — better than the
    date we happened to download it."""
    if "feed_info.txt" not in zf.namelist():
        return {}
    lines = zf.read("feed_info.txt").decode("utf-8-sig").strip().splitlines()
    if len(lines) < 2:
        return {}
    header = [h.strip() for h in lines[0].split(",")]
    values = [v.strip() for v in lines[1].split(",")]
    return dict(zip(header, values))


def _to_parquet(extract_dir: Path, out_dir: Path) -> dict:
    """
    Convert the CSVs we care about into Parquet, leaving every column as text.

    stop_times.txt alone is 151 MB of CSV. Re-parsing that on every query, on a
    mechanical drive, is the wrong shape of work; Parquet is read once and
    scanned column by column.
    """
    import duckdb

    def literal(path: Path) -> str:
        # COPY ... TO will not take a bound parameter for its destination, so
        # the paths go in as SQL literals. Forward slashes work on Windows and
        # avoid any backslash ambiguity; doubling quotes is the standard escape.
        return "'" + str(path).replace("\\", "/").replace("'", "''") + "'"

    stats = {}
    con = duckdb.connect()
    try:
        for name in TABLES:
            csv_path = extract_dir / f"{name}.txt"
            if not csv_path.exists():
                continue
            parquet_path = out_dir / f"{name}.parquet"
            con.execute(
                f"COPY (SELECT * FROM read_csv({literal(csv_path)}, "
                f"header=true, all_varchar=true)) "
                f"TO {literal(parquet_path)} (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
            rows = con.execute(
                f"SELECT count(*) FROM read_parquet({literal(parquet_path)})"
            ).fetchone()[0]
            stats[name] = {"rows": rows, "bytes": parquet_path.stat().st_size}
    finally:
        con.close()
    return stats


def download(force: bool = False) -> dict:
    """
    Fetch the bundle if it has changed, and record it as a version.

    Sends the stored ETag so an unchanged bundle costs one request instead of
    55 MB. Falls back to hashing the download, because a server can return 200
    with identical bytes.
    """
    manifest = load_manifest()
    known = manifest["versions"]
    latest = max(known, key=lambda v: v["first_fetched_at"]) if known else None

    request = urllib.request.Request(URL, headers={"User-Agent": USER_AGENT})
    if latest and not force:
        if latest.get("etag"):
            request.add_header("If-None-Match", latest["etag"])
        elif latest.get("last_modified"):
            request.add_header("If-Modified-Since", latest["last_modified"])

    now = datetime.now(timezone.utc).isoformat()

    try:
        with urllib.request.urlopen(request, timeout=300) as response:
            payload = response.read()
            etag = response.headers.get("ETag")
            last_modified = response.headers.get("Last-Modified")
    except urllib.error.HTTPError as exc:
        if exc.code == 304:
            latest["last_checked_at"] = now
            save_manifest(manifest)
            return {"status": "unchanged", "version": latest}
        raise

    digest = hashlib.sha256(payload).hexdigest()
    for version in known:
        if version["sha256"] == digest:
            # Server didn't honour the conditional request, but the bytes are
            # identical, so this is not a new version.
            version["last_checked_at"] = now
            version["etag"] = etag or version.get("etag")
            save_manifest(manifest)
            return {"status": "unchanged", "version": version}

    with zipfile.ZipFile(io.BytesIO(payload)) as zf:
        info = _read_feed_info(zf)
        feed_version = info.get("feed_version") or "unknown"
        version_id = f"{feed_version}_{digest[:8]}"
        out_dir = GTFS_DIR / version_id
        out_dir.mkdir(parents=True, exist_ok=True)

        (out_dir / "bundle.zip").write_bytes(payload)

        # Extract to a temporary place, convert, then discard the CSVs: the zip
        # is the copy worth keeping, and 192 MB of extracted text is not.
        with tempfile.TemporaryDirectory() as tmp:
            extract_dir = Path(tmp)
            for name in TABLES:
                member = f"{name}.txt"
                if member in zf.namelist():
                    zf.extract(member, extract_dir)
            tables = _to_parquet(extract_dir, out_dir)

    record = {
        "version_id": version_id,
        "feed_version": feed_version,
        "feed_start_date": info.get("feed_start_date", ""),
        "feed_end_date": info.get("feed_end_date", ""),
        "sha256": digest,
        "etag": etag,
        "last_modified": last_modified,
        "first_fetched_at": now,
        "last_checked_at": now,
        "zip_bytes": len(payload),
        "tables": tables,
        "path": str(out_dir.relative_to(ROOT)).replace("\\", "/"),
    }
    known.append(record)
    save_manifest(manifest)
    return {"status": "new", "version": record}


# --------------------------------------------------------------------------
# cli
# --------------------------------------------------------------------------

def _print_version(v: dict) -> None:
    print(f"  {v['feed_version']:12s} {v['feed_start_date']} - {v['feed_end_date']}  "
          f"fetched {v['first_fetched_at'][:10]}  {v['zip_bytes']/1e6:,.0f} MB")
    for name, stat in sorted(v.get("tables", {}).items(), key=lambda kv: -kv[1]["rows"]):
        print(f"      {name:18s} {stat['rows']:>10,} rows  {stat['bytes']/1e6:>7,.1f} MB")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--list", action="store_true", help="show held versions")
    parser.add_argument("--force", action="store_true", help="download even if unchanged")
    args = parser.parse_args()

    if args.list:
        manifest = load_manifest()
        if not manifest["versions"]:
            print("No bundles held yet. Run without --list to fetch one.")
            return 0
        print(f"{len(manifest['versions'])} bundle(s) in {GTFS_DIR}:")
        for version in sorted(manifest["versions"], key=lambda v: v["first_fetched_at"]):
            _print_version(version)
        return 0

    print(f"Checking {URL}")
    result = download(force=args.force)
    version = result["version"]

    if result["status"] == "unchanged":
        record(f"unchanged - still {version['feed_version']} "
               f"({version['feed_start_date']} to {version['feed_end_date']})")
    else:
        record(f"NEW BUNDLE {version['feed_version']} "
               f"({version['feed_start_date']} to {version['feed_end_date']}), "
               f"{version['zip_bytes']/1e6:.0f} MB -> {version['path']}")
        _print_version(version)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


# --------------------------------------------------------------------------
# choosing a bundle by measurement rather than by its own metadata
# --------------------------------------------------------------------------

DEFAULT_PREDICTIONS = str(
    ROOT / "data" / "parquet" / "predictions" / "dt=*" / "part.parquet"
).replace("\\", "/")

MINIMUM_MATCH = 90.0


def measure_match(day, bundle: dict, predictions_glob: str = DEFAULT_PREDICTIONS,
                  root: Path = ROOT) -> float:
    """
    Share of the trip_ids collected on `day` that exist in this bundle.

    trip_id is an internal identifier. An agency regenerates them when it
    republishes, so overlap is the only honest test of whether a bundle
    describes the data.
    """
    import duckdb

    stop_times = str(root / bundle["path"] / "stop_times.parquet").replace("\\", "/")
    if not Path(stop_times).exists():
        return 0.0

    wanted = _as_date(day).strftime("%Y%m%d")
    con = duckdb.connect()
    try:
        con.execute("SET enable_progress_bar=false")
        return con.execute(f"""
            SELECT 100.0 * count(DISTINCT CASE WHEN s.trip_id IS NOT NULL
                                               THEN t.trip_id END)
                   / nullif(count(DISTINCT t.trip_id), 0)
            FROM (SELECT DISTINCT trip_id FROM read_parquet('{predictions_glob}')
                  WHERE start_date = '{wanted}') t
            LEFT JOIN (SELECT DISTINCT trip_id
                       FROM read_parquet('{stop_times}')) s USING (trip_id)
        """).fetchone()[0] or 0.0
    finally:
        con.close()


def bundle_for_measured(day, predictions_glob: str = DEFAULT_PREDICTIONS,
                        manifest: dict | None = None,
                        minimum: float = MINIMUM_MATCH,
                        root: Path = ROOT) -> dict:
    """
    Return the bundle whose trip_ids actually match the data collected on `day`.

    This is what production code should call. bundle_for() reads the dates a
    bundle declares about itself, and on 2026-09-28 that was shown to be
    worthless: a bundle declaring coverage through 2026-10-10 matched 31% of
    the trip_ids being collected, and the join quietly returned almost nothing.

    Raises LookupError when nothing clears `minimum`, listing what was tried.
    Refusing is the point — a delay computed against the wrong timetable looks
    entirely normal and cannot be detected downstream.
    """
    manifest = manifest if manifest is not None else load_manifest()
    scored = [
        (measure_match(day, version, predictions_glob, root), version)
        for version in manifest["versions"]
    ]
    if scored:
        rate, best = max(scored, key=lambda pair: pair[0])
        if rate >= minimum:
            return {**best, "match_rate": rate, "approximate": False}

    tried = ", ".join(
        f"{version['feed_version']} {rate:.1f}%" for rate, version in scored
    ) or "none held"
    raise LookupError(
        f"No timetable matches the data collected on {_as_date(day)}. "
        f"Tried: {tried}. Minimum is {minimum:.0f}%. "
        f"The version in force that day has to be fetched before delay can be "
        f"computed for it."
    )
