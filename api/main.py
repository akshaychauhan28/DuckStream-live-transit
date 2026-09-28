"""
The public demo: a question in plain English, an answer from collected data.

    .venv\\Scripts\\python.exe -m uvicorn api.main:app --reload

    question -> nl_to_sql -> guardrails.validate -> read-only DuckDB -> rows

Everything defensive lives in guardrails.py. This file is the plumbing around
it: one page, one endpoint, a cache, and a rate limit.

The cache and the limit exist for the same reason. Groq's free tier is capped
per minute and per day, and a link that reaches a few hundred people would
otherwise exhaust it in an afternoon. Repeated questions are common on a demo,
and a cached answer costs nothing.
"""

import os
import sys
import threading
import time
from collections import defaultdict, deque
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

sys.path.insert(0, str(Path(__file__).resolve().parent))

from guardrails import GuardrailError, open_connection, run  # noqa: E402
from nl_to_sql import ModelError, describe, generate_sql  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent

# Load .env when running locally, if python-dotenv happens to be installed.
# On Render the values come from the dashboard, so this is a convenience and
# never a dependency - it is deliberately absent from api/requirements.txt.
try:
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
except ImportError:
    pass

DB_PATH = os.environ.get("DEMO_DB", str(ROOT / "data" / "demo.duckdb"))

ROW_LIMIT = 100
QUERY_TIMEOUT = 10.0
CACHE_SIZE = 500
RATE_LIMIT = 20          # questions per client
RATE_WINDOW = 300        # seconds

app = FastAPI(title="DuckStream", docs_url=None, redoc_url=None)

# The stop list is a few hundred kilobytes of JSON and compresses to a fraction
# of that, which matters on a free instance.
app.add_middleware(GZipMiddleware, minimum_size=1000)

# One DuckDB connection is shared by every request, and FastAPI runs sync
# endpoints in a thread pool, so two questions arriving together would use it
# concurrently. Serialising is the right trade here: queries are milliseconds
# and the alternative is a connection per request against a file that never
# changes.
_db_lock = threading.Lock()

_cache: dict[str, dict] = {}
_seen: deque = deque()
_hits: dict[str, deque] = defaultdict(deque)

_con = None
_schema = ""
_about = ""


class Ask(BaseModel):
    question: str


def connection():
    """Opened once, read-only, external access off. See guardrails.py."""
    global _con, _schema, _about
    if _con is None:
        _con = open_connection(DB_PATH)
        _schema, _about = describe(_con)
    return _con


def allowed(client: str) -> bool:
    now = time.time()
    hits = _hits[client]
    while hits and now - hits[0] > RATE_WINDOW:
        hits.popleft()
    if len(hits) >= RATE_LIMIT:
        return False
    hits.append(now)
    return True


def remember(key: str, value: dict) -> dict:
    _cache[key] = value
    _seen.append(key)
    while len(_seen) > CACHE_SIZE:
        _cache.pop(_seen.popleft(), None)
    return value


@app.get("/health")
def health():
    con = connection()
    with _db_lock:
        rows = con.execute("SELECT count(*) FROM arrivals").fetchone()[0]
    return {"status": "ok", "arrivals": rows}


# Stops with fewer arrivals than this are left off the map. Their medians move
# on a handful of buses and would read as real hotspots.
MIN_ARRIVALS = 30

_stops_cache: list | None = None


@app.get("/stops")
def stops():
    """Every stop worth plotting, with how reliable it has been."""
    global _stops_cache
    if _stops_cache is None:
        con = connection()
        with _db_lock:
            rows = con.execute(f"""
                SELECT stop_id, any_value(stop_name), any_value(stop_lat),
                       any_value(stop_lon), count(*),
                       CAST(median(delay_seconds) AS INTEGER),
                       CAST(100.0 * sum(CASE WHEN abs(delay_seconds) <= 60
                                             THEN 1 ELSE 0 END)
                            / count(*) AS INTEGER)
                FROM arrivals
                WHERE stop_lat IS NOT NULL
                GROUP BY stop_id
                HAVING count(*) >= {MIN_ARRIVALS}
            """).fetchall()
        # Short keys because this is a few thousand rows going over the wire.
        _stops_cache = [
            {"id": r[0], "n": r[1], "la": r[2], "lo": r[3],
             "c": r[4], "d": r[5], "p": r[6]}
            for r in rows
        ]
    return {"stops": _stops_cache, "min_arrivals": MIN_ARRIVALS}


@app.get("/stop/{stop_id}")
def stop_detail(stop_id: str):
    """How reliable one stop has been, by hour and by route."""
    con = connection()
    with _db_lock:
        summary = con.execute("""
            SELECT any_value(stop_name), count(*),
                   CAST(median(delay_seconds) AS INTEGER),
                   CAST(100.0 * sum(CASE WHEN abs(delay_seconds) <= 60
                                         THEN 1 ELSE 0 END) / count(*) AS INTEGER),
                   CAST(100.0 * sum(CASE WHEN delay_seconds > 300
                                         THEN 1 ELSE 0 END) / count(*) AS INTEGER)
            FROM arrivals WHERE stop_id = ?
        """, [stop_id]).fetchone()

        if not summary or not summary[1]:
            return JSONResponse({"error": "No arrivals recorded at that stop."},
                                status_code=404)

        by_hour = con.execute("""
            SELECT hour_local, count(*), CAST(median(delay_seconds) AS INTEGER)
            FROM arrivals WHERE stop_id = ?
            GROUP BY hour_local HAVING count(*) >= 5 ORDER BY hour_local
        """, [stop_id]).fetchall()

        by_route = con.execute("""
            SELECT route_name, count(*), CAST(median(delay_seconds) AS INTEGER),
                   CAST(100.0 * sum(CASE WHEN delay_seconds > 300
                                         THEN 1 ELSE 0 END) / count(*) AS INTEGER)
            FROM arrivals WHERE stop_id = ? AND route_name IS NOT NULL
            GROUP BY route_name HAVING count(*) >= 5
            ORDER BY median(delay_seconds) DESC LIMIT 10
        """, [stop_id]).fetchall()

    return {
        "stop_id": stop_id,
        "stop_name": summary[0],
        "arrivals": summary[1],
        "median_delay": summary[2],
        "pct_on_time": summary[3],
        "pct_very_late": summary[4],
        "by_hour": [{"h": h, "n": n, "d": d} for h, n, d in by_hour],
        "by_route": [{"route": r, "n": n, "d": d, "late": late}
                     for r, n, d, late in by_route],
    }


@app.post("/ask")
def ask(body: Ask, request: Request):
    question = body.question.strip()
    if not question:
        return JSONResponse({"error": "Ask a question first."}, status_code=400)
    if len(question) > 500:
        return JSONResponse({"error": "That question is too long."}, status_code=400)

    client = request.client.host if request.client else "unknown"
    if not allowed(client):
        return JSONResponse(
            {"error": "Too many questions from here. Try again in a few minutes."},
            status_code=429,
        )

    key = " ".join(question.lower().split())
    if key in _cache:
        return {**_cache[key], "cached": True}

    con = connection()
    try:
        sql = generate_sql(question, _schema, _about)
    except ModelError as exc:
        return JSONResponse({"error": str(exc)}, status_code=503)

    try:
        with _db_lock:
            rows = run(con, sql, limit=ROW_LIMIT, timeout=QUERY_TIMEOUT)
            columns = [d[0] for d in con.description] if con.description else []
    except GuardrailError as exc:
        # The generated SQL is returned either way. The point of the project is
        # that an answer can be checked, including when it is refused.
        return JSONResponse({"error": str(exc), "sql": sql}, status_code=400)

    answer = {
        "sql": sql,
        "columns": columns,
        "rows": [list(r) for r in rows],
        "cached": False,
    }
    return remember(key, answer)


# The page lives in its own file. It had outgrown being a Python string, and
# an editor is a great deal more useful when it knows it is looking at HTML.
PAGE = (Path(__file__).resolve().parent / "page.html").read_text(encoding="utf-8")


@app.get("/", response_class=HTMLResponse)
def home():
    return PAGE
