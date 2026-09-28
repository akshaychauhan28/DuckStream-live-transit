"""
Tests for the demo endpoint, with the language model stubbed out.

The model is the one part that cannot be tested deterministically, so it is
replaced here. Everything else is real: the guardrail, the read-only
connection, the cache and the rate limit.
"""

import sys
from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "api"))


@pytest.fixture
def client(tmp_path, monkeypatch):
    db = tmp_path / "demo.duckdb"
    con = duckdb.connect(str(db))
    con.execute("""
        CREATE TABLE arrivals AS
        SELECT 'route ' || (i % 3) AS route_name,
               'stop ' || (i % 7)  AS stop_name,
               i * 11 - 200        AS delay_seconds,
               (i % 24)            AS hour_local
        FROM range(300) t(i)
    """)
    con.execute("CREATE TABLE about (topic VARCHAR, detail VARCHAR)")
    con.execute("INSERT INTO about VALUES ('delay_seconds', 'Positive means late.')")
    con.close()

    monkeypatch.setenv("DEMO_DB", str(db))
    for module in ("main", "nl_to_sql", "guardrails"):
        sys.modules.pop(module, None)
    import main

    main._cache.clear()
    main._seen.clear()
    main._hits.clear()
    main._con = None
    return TestClient(main.app), main


def stub(main, sql):
    """Replace the model with a fixed answer."""
    main.generate_sql = lambda question, schema, about="": sql


def test_health_reports_row_count(client):
    app, main = client
    body = app.get("/health").json()
    assert body["status"] == "ok"
    assert body["arrivals"] == 300


def test_home_page_renders(client):
    app, _ = client
    page = app.get("/")
    assert page.status_code == 200
    assert "Ask Ottawa's buses" in page.text


def test_a_question_returns_rows_and_the_sql(client):
    app, main = client
    stub(main, "SELECT route_name, median(delay_seconds) AS d "
                "FROM arrivals GROUP BY 1 ORDER BY 2 DESC")
    body = app.post("/ask", json={"question": "which route is worst?"}).json()

    assert body["columns"] == ["route_name", "d"]
    assert len(body["rows"]) == 3
    assert "SELECT" in body["sql"]
    assert body["cached"] is False


def test_an_empty_question_is_refused(client):
    app, _ = client
    assert app.post("/ask", json={"question": "   "}).status_code == 400


def test_a_very_long_question_is_refused(client):
    app, _ = client
    reply = app.post("/ask", json={"question": "x" * 501})
    assert reply.status_code == 400


def test_dangerous_sql_from_the_model_is_refused(client):
    """The model is untrusted. The guardrail is what stands behind it."""
    app, main = client
    stub(main, "DROP TABLE arrivals")
    reply = app.post("/ask", json={"question": "delete everything"})

    assert reply.status_code == 400
    assert "SELECT" in reply.json()["error"]
    # and the table is still there
    assert app.get("/health").json()["arrivals"] == 300


def test_file_reading_sql_is_refused(client):
    app, main = client
    stub(main, "SELECT * FROM read_csv('/etc/passwd')")
    reply = app.post("/ask", json={"question": "read a file"})
    assert reply.status_code == 400


def test_the_row_limit_is_applied(client):
    app, main = client
    stub(main, "SELECT * FROM arrivals")
    body = app.post("/ask", json={"question": "everything"}).json()
    assert len(body["rows"]) == main.ROW_LIMIT


def test_repeated_questions_are_cached(client):
    app, main = client
    calls = {"n": 0}

    def counting(question, schema, about=""):
        calls["n"] += 1
        return "SELECT 1 AS x"

    main.generate_sql = counting
    app.post("/ask", json={"question": "How late is route 1?"})
    second = app.post("/ask", json={"question": "  how LATE is route 1?  "}).json()

    assert calls["n"] == 1, "the model was asked twice for the same question"
    assert second["cached"] is True


def test_the_rate_limit_stops_a_flood(client):
    app, main = client
    stub(main, "SELECT 1 AS x")
    for i in range(main.RATE_LIMIT):
        assert app.post("/ask", json={"question": f"q{i}"}).status_code == 200
    blocked = app.post("/ask", json={"question": "one too many"})
    assert blocked.status_code == 429


def test_a_model_failure_is_reported_not_crashed(client):
    app, main = client
    from nl_to_sql import ModelError

    def failing(question, schema, about=""):
        raise ModelError("GROQ_API_KEY is not set.")

    main.generate_sql = failing
    reply = app.post("/ask", json={"question": "anything"})
    assert reply.status_code == 503
    assert "GROQ_API_KEY" in reply.json()["error"]
