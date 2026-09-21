"""
Adversarial tests for the SQL guardrail.

Written as attacks rather than examples, because the thing being defended
against is a language model producing SQL nobody reviewed, on a public URL.

Anything that gets through here is a bug to fix before deploying, not a
curiosity.
"""

import sys
import time
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "api"))

from guardrails import (  # noqa: E402
    GuardrailError,
    open_connection,
    run,
    validate,
)


@pytest.fixture(scope="module")
def demo_db(tmp_path_factory) -> str:
    """A database shaped like the one the demo would serve."""
    path = tmp_path_factory.mktemp("guard") / "demo.duckdb"
    con = duckdb.connect(str(path))
    con.execute("""
        CREATE TABLE arrivals AS
        SELECT i AS id, 'route ' || (i % 5) AS route, i * 7 AS delay
        FROM range(1000) t(i)
    """)
    con.close()
    return str(path)


@pytest.fixture(scope="module")
def secret(tmp_path_factory) -> str:
    path = tmp_path_factory.mktemp("guard") / "secret.txt"
    path.write_text("TOP SECRET\n", encoding="utf-8")
    return path.as_posix()


@pytest.fixture
def con(demo_db):
    connection = open_connection(demo_db)
    yield connection
    connection.close()


# --------------------------------------------------------------------------
# what should work
# --------------------------------------------------------------------------

def test_a_plain_select_is_allowed(con):
    rows = run(con, "SELECT route, count(*) FROM arrivals GROUP BY route")
    assert len(rows) == 5


def test_a_cte_is_allowed(con):
    rows = run(con, "WITH x AS (SELECT * FROM arrivals) SELECT count(*) FROM x")
    assert rows[0][0] == 1000


def test_validate_returns_the_query(con):
    assert "arrivals" in validate("SELECT * FROM arrivals")


# --------------------------------------------------------------------------
# statement-level attacks
# --------------------------------------------------------------------------

@pytest.mark.parametrize("sql", [
    "DROP TABLE arrivals",
    "CREATE TABLE evil (x INT)",
    "INSERT INTO arrivals VALUES (1, 'x', 2)",
    "UPDATE arrivals SET delay = 0",
    "DELETE FROM arrivals",
    "ATTACH 'other.db'",
    "INSTALL httpfs",
    "LOAD httpfs",
    "PRAGMA database_list",
    "SET enable_external_access=true",
    "COPY (SELECT 1) TO 'out.csv'",
    "EXPORT DATABASE 'dump'",
])
def test_non_select_statements_are_refused(sql):
    with pytest.raises(GuardrailError):
        validate(sql)


@pytest.mark.parametrize("sql", [
    "SELECT 1; DROP TABLE arrivals",
    "SELECT 1; SELECT 2",
    "SELECT 1; -- trailing\nDROP TABLE arrivals",
    "SELECT 1 /* comment */; CREATE TABLE evil (x INT)",
])
def test_stacked_statements_are_refused(sql):
    """One question means one statement. Anything after the semicolon is an attack."""
    with pytest.raises(GuardrailError):
        validate(sql)


def test_trailing_semicolons_are_harmless():
    """`SELECT 1;;` parses as one statement, so refusing it would be theatre."""
    assert validate("SELECT 1;;")


@pytest.mark.parametrize("sql", [
    "PRAGMA database_list",
    "PRAGMA show_tables",
    "SELECT * FROM duckdb_settings()",
    "SELECT * FROM duckdb_tables()",
    "SELECT * FROM pragma_database_list()",
])
def test_duckdb_internals_are_refused(sql):
    """
    DuckDB rewrites PRAGMA into a table function, so these arrive as ordinary
    SELECTs and the statement-type check cannot see them. They expose file
    paths, settings and the schema.
    """
    with pytest.raises(GuardrailError):
        validate(sql)


@pytest.mark.parametrize("sql", [
    "-- just a comment",
    "",
    "   ",
    "this is not sql at all",
    "SELECT FROM WHERE",
])
def test_nonsense_is_refused(sql):
    with pytest.raises(GuardrailError):
        validate(sql)


def test_a_comment_cannot_hide_a_second_statement():
    """Text matching on ';' or keywords is not parsing; this is why."""
    with pytest.raises(GuardrailError):
        validate("SELECT 1 -- ; DROP TABLE arrivals\n; DROP TABLE arrivals")


# --------------------------------------------------------------------------
# a valid SELECT can still be an attack
# --------------------------------------------------------------------------

def test_a_select_cannot_read_local_files(con, secret):
    """
    read_csv makes this a perfectly valid SELECT, so statement checking alone
    would pass it. The connection settings are what stop it.
    """
    with pytest.raises(Exception) as excinfo:
        run(con, f"SELECT * FROM read_csv('{secret}', header=false)")
    assert "TOP SECRET" not in str(excinfo.value)


def test_a_select_cannot_glob_the_filesystem(con):
    with pytest.raises(Exception):
        run(con, "SELECT * FROM read_parquet('*.parquet')")


def test_the_connection_cannot_write_files(con, tmp_path):
    target = (tmp_path / "written.csv").as_posix()
    with pytest.raises(Exception):
        run(con, f"COPY (SELECT 1) TO '{target}'")
    assert not Path(target).exists(), "a file was written to disk"


def test_the_connection_cannot_be_unlocked(con):
    with pytest.raises(Exception):
        run(con, "SET enable_external_access=true")


# --------------------------------------------------------------------------
# resource limits
# --------------------------------------------------------------------------

def test_a_limit_is_applied_even_when_the_query_has_none(con):
    rows = run(con, "SELECT * FROM arrivals", limit=10)
    assert len(rows) == 10


def test_a_limit_is_applied_when_the_query_asks_for_more(con):
    rows = run(con, "SELECT * FROM arrivals LIMIT 900", limit=10)
    assert len(rows) == 10


def test_a_smaller_limit_in_the_query_is_respected(con):
    rows = run(con, "SELECT * FROM arrivals LIMIT 3", limit=100)
    assert len(rows) == 3


def test_a_slow_query_is_stopped(con):
    """A model can write a query that never finishes. It must not hold the API."""
    started = time.perf_counter()
    with pytest.raises(GuardrailError):
        run(con, "SELECT count(*) FROM range(1000000000000)", timeout=2.0)
    assert time.perf_counter() - started < 15, "the timeout did not fire"


def test_the_connection_still_works_after_a_timeout(con):
    with pytest.raises(GuardrailError):
        run(con, "SELECT count(*) FROM range(1000000000000)", timeout=2.0)
    rows = run(con, "SELECT count(*) FROM arrivals")
    assert rows[0][0] == 1000


# --------------------------------------------------------------------------
# what the user sees
# --------------------------------------------------------------------------

def test_refusals_do_not_leak_the_schema():
    """An error message goes to a stranger on the internet."""
    with pytest.raises(GuardrailError) as excinfo:
        validate("DROP TABLE arrivals")
    message = str(excinfo.value).lower()
    assert "arrivals" not in message
    assert "traceback" not in message
