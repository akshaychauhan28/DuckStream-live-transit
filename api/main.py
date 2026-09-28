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
import time
from collections import defaultdict, deque
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

sys.path.insert(0, str(Path(__file__).resolve().parent))

from guardrails import GuardrailError, open_connection, run  # noqa: E402
from nl_to_sql import ModelError, describe, generate_sql  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = os.environ.get("DEMO_DB", str(ROOT / "data" / "demo.duckdb"))

ROW_LIMIT = 100
QUERY_TIMEOUT = 10.0
CACHE_SIZE = 500
RATE_LIMIT = 20          # questions per client
RATE_WINDOW = 300        # seconds

app = FastAPI(title="DuckStream", docs_url=None, redoc_url=None)

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
    rows = con.execute("SELECT count(*) FROM arrivals").fetchone()[0]
    return {"status": "ok", "arrivals": rows}


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
        rows = run(con, sql, limit=ROW_LIMIT, timeout=QUERY_TIMEOUT)
    except GuardrailError as exc:
        # The generated SQL is returned either way. The point of the project is
        # that an answer can be checked, including when it is refused.
        return JSONResponse({"error": str(exc), "sql": sql}, status_code=400)

    columns = [d[0] for d in con.description] if con.description else []
    answer = {
        "sql": sql,
        "columns": columns,
        "rows": [list(r) for r in rows],
        "cached": False,
    }
    return remember(key, answer)


PAGE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>DuckStream &mdash; ask Ottawa transit data</title>
<style>
 :root { color-scheme: light dark; }
 body { font: 16px/1.5 system-ui, sans-serif; max-width: 52rem; margin: 0 auto;
        padding: 2rem 1rem; }
 h1 { font-size: 1.4rem; margin-bottom: .25rem; }
 p.sub { color: #777; margin-top: 0; }
 form { display: flex; gap: .5rem; margin: 1.5rem 0 1rem; }
 input { flex: 1; padding: .6rem .7rem; font: inherit; border: 1px solid #999;
         border-radius: 6px; background: transparent; color: inherit; }
 button { padding: .6rem 1.1rem; font: inherit; border: 0; border-radius: 6px;
          background: #2563eb; color: #fff; cursor: pointer; }
 button:disabled { opacity: .5; cursor: default; }
 .examples button { background: transparent; color: #2563eb;
                    border: 1px solid #2563eb; padding: .3rem .6rem;
                    font-size: .85rem; margin: 0 .4rem .4rem 0; }
 pre { background: #8881; padding: .7rem; border-radius: 6px; overflow-x: auto;
       font-size: .85rem; }
 table { border-collapse: collapse; width: 100%; font-size: .9rem; }
 th, td { text-align: left; padding: .35rem .6rem; border-bottom: 1px solid #8883; }
 .err { color: #b91c1c; }
 footer { margin-top: 2.5rem; font-size: .85rem; color: #777; }
</style></head><body>
<h1>Ask Ottawa's buses a question</h1>
<p class="sub">Real arrivals, collected every 45 seconds from the live OC Transpo
feed and joined to the published timetable.</p>

<form id="f">
  <input id="q" placeholder="Which routes are latest at 5pm?" autocomplete="off">
  <button id="go">Ask</button>
</form>

<div class="examples">
  <button type="button">Which routes are latest at 5pm?</button>
  <button type="button">What hour of the day has the worst delays?</button>
  <button type="button">Which stops do buses arrive early at?</button>
  <button type="button">How many arrivals were within a minute of schedule?</button>
</div>

<div id="out"></div>

<footer>
  The SQL is written by a language model, then checked before it runs: one
  SELECT, read-only, no file access, a row limit and a timeout.
  <a href="https://github.com/akshaychauhan28/DuckStream-live-transit">Source</a>.
</footer>

<script>
const out = document.getElementById("out");
const q = document.getElementById("q");
const go = document.getElementById("go");
const form = document.getElementById("f");

function esc(s) {
  return String(s).replace(/[&<>"']/g, function (c) {
    return {"&": "&amp;", "<": "&lt;", ">": "&gt;",
            '"': "&quot;", "'": "&#39;"}[c];
  });
}

document.querySelectorAll(".examples button").forEach(function (b) {
  b.onclick = function () { q.value = b.textContent; form.requestSubmit(); };
});

form.onsubmit = async function (e) {
  e.preventDefault();
  if (!q.value.trim()) return;
  go.disabled = true;
  out.innerHTML = "<p>Thinking...</p>";
  try {
    const r = await fetch("/ask", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({question: q.value})
    });
    const d = await r.json();
    let html = "";
    if (d.sql) html += "<pre>" + esc(d.sql) + "</pre>";
    if (d.error) {
      html += '<p class="err">' + esc(d.error) + "</p>";
    } else if (d.rows) {
      if (!d.rows.length) {
        html += "<p>No rows came back.</p>";
      } else {
        html += "<table><tr>";
        d.columns.forEach(function (c) { html += "<th>" + esc(c) + "</th>"; });
        html += "</tr>";
        d.rows.forEach(function (row) {
          html += "<tr>";
          row.forEach(function (v) {
            html += "<td>" + esc(v === null ? "" : v) + "</td>";
          });
          html += "</tr>";
        });
        html += "</table>";
      }
      if (d.cached) html += '<p class="sub">(cached)</p>';
    }
    out.innerHTML = html;
  } catch (err) {
    out.innerHTML = '<p class="err">Something went wrong. Try again.</p>';
  }
  go.disabled = false;
};
</script>
</body></html>"""


@app.get("/", response_class=HTMLResponse)
def home():
    return PAGE
