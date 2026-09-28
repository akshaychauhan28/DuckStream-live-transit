"""
Recompute every number quoted publicly, from the data itself.

    python scripts/verify_claims.py

Posts link the repo, so the numbers in them have to be reproducible by anyone
who clones it. This prints each claim beside what the data actually says.
"""

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "capture"))

import duckdb  # noqa: E402

from frames import read_frames  # noqa: E402


def show(label, measured, claimed=None):
    mark = ""
    if claimed is not None:
        mark = "  <-- CLAIMED " + str(claimed)
    print(f"  {label:<34} {measured}{mark}")


print("RAW ARCHIVE")
raw_files = sorted((ROOT / "raw").glob("capture_*.gz"))
raw_bytes = sum(p.stat().st_size for p in raw_files)
show("files", f"{len(raw_files):,}")
show("size on disk", f"{raw_bytes/1e6:,.1f} MB", "717 MB")

first = last = None
vp_times, vp_fail = [], 0
CUT = datetime(2026, 9, 17, 10, 30, tzinfo=timezone.utc)
for path in raw_files:
    for meta, _ in read_frames(path):
        stamp = meta.get("fetched_at")
        if not stamp:
            continue
        when = datetime.fromisoformat(stamp)
        first = when if first is None or when < first else first
        last = when if last is None or when > last else last
        if meta.get("feed") == "vehicle_positions" and when >= CUT:
            vp_times.append(when)
            if meta.get("status") != 200:
                vp_fail += 1

show("first frame", f"{first:%Y-%m-%d %H:%M} UTC")
show("last frame", f"{last:%Y-%m-%d %H:%M} UTC")
days = (datetime.now(timezone.utc) - first).total_seconds() / 86400
show("days since collection started", f"{days:.1f}", "day 14 of 30")

vp_times.sort()
span = (vp_times[-1] - vp_times[0]).total_seconds()
gaps = [(b - a).total_seconds() for a, b in zip(vp_times, vp_times[1:])]
show("phone-only window", f"{span/86400:.1f} days", "10 days")
show("phone-only coverage", f"{100*len(vp_times)/(span/45):.1f}%", "99.9%")
show("failed polls", f"{100*vp_fail/len(vp_times):.2f}%")
show("longest gap", f"{max(gaps):.0f}s")

print("\nPARQUET")
con = duckdb.connect()
con.execute("SET enable_progress_bar=false")
pq = ROOT / "data" / "parquet"
total_rows = 0
for table in ("vehicle_positions", "trips", "predictions"):
    glob = str(pq / table / "dt=*" / "part.parquet").replace("\\", "/")
    n = con.execute(f"SELECT count(*) FROM read_parquet('{glob}')").fetchone()[0]
    total_rows += n
    show(f"{table} rows", f"{n:,}")
pq_bytes = sum(p.stat().st_size for p in pq.rglob("*.parquet"))
show("total rows", f"{total_rows:,}")
show("parquet size", f"{pq_bytes/1e6:,.1f} MB", "200 MB")

build_log = ROOT / "logs" / "build.log"
if build_log.exists():
    raw_rows = sum(int(line.split()[3].replace(",", ""))
                   for line in build_log.read_text().splitlines()
                   if line[:4].isdigit())
    show("raw rows processed", f"{raw_rows:,}", "83 million")

print("\nARRIVALS PER DAY (demo.duckdb)")
demo = ROOT / "data" / "demo.duckdb"
if demo.exists():
    d = duckdb.connect(str(demo), read_only=True)
    # service_date is a DATE, so format it rather than comparing to a string.
    # This block silently printed zeros for a while after that column changed
    # type, which is the exact failure this script exists to catch.
    counts = d.execute("""
        SELECT strftime(service_date, '%Y-%m-%d'), count(*)
        FROM arrivals GROUP BY 1 ORDER BY 1
    """).fetchall()
    total = sum(n for _, n in counts)
    d.close()
    for day, n in counts:
        show(day, f"{n:,}")
    show("days with arrivals", f"{len(counts)}")
    show("total arrivals", f"{total:,}", "585,201")

print("\nTRIP ID MATCH RATE")
preds = str(pq / "predictions" / "dt=*" / "part.parquet").replace("\\", "/")
manifest = json.loads((ROOT / "data" / "gtfs" / "manifest.json").read_text())
dates = [r[0] for r in con.execute(
    f"SELECT DISTINCT start_date FROM read_parquet('{preds}') "
    f"WHERE start_date IS NOT NULL ORDER BY 1").fetchall()]
header = "  " + f"{'date':<12}" + "".join(
    f"{v['feed_version']:>12}" for v in manifest["versions"])
print(header)
best = {}
for day in dates:
    line = f"  {day:<12}"
    for version in manifest["versions"]:
        st = str(ROOT / version["path"] / "stop_times.parquet").replace("\\", "/")
        pct = con.execute(f"""
            SELECT 100.0 * count(DISTINCT CASE WHEN s.trip_id IS NOT NULL
                                               THEN t.trip_id END)
                   / nullif(count(DISTINCT t.trip_id), 0)
            FROM (SELECT DISTINCT trip_id FROM read_parquet('{preds}')
                  WHERE start_date = '{day}') t
            LEFT JOIN (SELECT DISTINCT trip_id FROM read_parquet('{st}')) s
              USING (trip_id)
        """).fetchone()[0] or 0
        best[day] = max(best.get(day, 0), pct)
        line += f"{pct:>11.1f}%"
    print(line)
con.close()

good = [d for d, p in best.items() if p >= 90]
bad = [d for d, p in best.items() if p < 90]
print()
show("days matching a held timetable", f"{len(good)}")
show("days matching nothing", f"{len(bad)}  ({min(bad)} to {max(bad)})" if bad else "0")
show("worst match among those days", f"{max(best[d] for d in bad):.1f}%" if bad else "-",
     "under 39%")

print("\nREPO")
commits = subprocess.run(["git", "rev-list", "--count", "HEAD"],
                         capture_output=True, text=True, cwd=ROOT).stdout.strip()
show("commits", commits)
