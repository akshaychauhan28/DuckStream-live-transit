"""
Execute LLM-written SQL without handing over the machine.

Not implemented — Phase 4. Required before anything is deployed publicly.

THREAT MODEL

A public endpoint runs SQL written by a language model against DuckDB. DuckDB
reads and writes files and loads extensions, so this is closer to remote code
execution than to a typo problem. DuckDB's own documentation says to treat SQL
from untrusted sources as untrusted code.

MEASURED, NOT ASSUMED (duckdb 1.5.5, 2026-09-19)

A read-only connection is not enough. It protects the database, not the disk:

    read_only=True
      CREATE TABLE        blocked
      COPY (...) TO file  ALLOWED     wrote a file
      read_csv('secret')  ALLOWED     read a local file

Adding enable_external_access=false closes both, and blocks ATTACH and
INSTALL as well. With the database opened read-only, the setting cannot be
turned back on from inside a query either.

    read_only=True, enable_external_access=false
      read_csv('secret')          blocked   PermissionException
      read_parquet('*.parquet')   blocked
      ATTACH 'other.db'           blocked
      INSTALL httpfs              blocked
      SET enable_external_access  blocked

LAYERS

  1. The database is a file containing tables, not a view over the Parquet
     lake. Nothing legitimate needs to touch the filesystem at query time, so
     external access can be off entirely.
  2. read_only=True, enable_external_access=false, lock_configuration=true.
  3. Exactly one statement, and it must be a SELECT. Checked by parsing with
     duckdb.extract_statements, not by matching text: a query can begin with a
     CTE, hide a second statement after a comment, or nest intent in a
     subquery.
  4. A LIMIT is applied to every query regardless of what the model wrote.
  5. A timeout, enforced by interrupting the connection from another thread.

Layer 3 alone would not stop `SELECT * FROM read_csv('C:/secrets.txt')`, which
is a perfectly valid SELECT. Layer 2 alone would not stop a query that runs for
an hour. Neither is redundant.

CONTRACT

    open_connection(db_path)        -> a locked-down duckdb connection
    validate(sql)                   -> the sql, or raises GuardrailError
    run(con, sql, limit=, timeout=) -> list of rows, or raises GuardrailError

    GuardrailError messages are shown to users, so they must say what was
    refused without describing the schema or the internals.

Run the tests to check your work:

    .venv\\Scripts\\python.exe -m pytest tests/test_guardrails.py -v
"""

import re
import threading

import duckdb

# Applied to every connection. lock_configuration is belt and braces: a SET
# statement is not a SELECT and would already be refused by validate().
SAFE_CONFIG = {
    "enable_external_access": "false",
    "lock_configuration": "true",
}

DEFAULT_LIMIT = 200
DEFAULT_TIMEOUT = 10.0

# DuckDB's own catalog, reachable from an ordinary SELECT. `PRAGMA
# database_list` is rewritten by the parser into
# `SELECT * FROM pragma_database_list`, so it arrives as a SELECT and the
# statement-type check cannot see it. These functions expose file paths,
# settings and the full schema, none of which the demo needs.
INTERNALS = re.compile(r"\b(?:pragma|duckdb|sqlite)_\w+", re.IGNORECASE)


class GuardrailError(Exception):
    """Raised when a query is refused, or stopped while running."""


def open_connection(db_path: str) -> duckdb.DuckDBPyConnection:
    """
    Open the demo database read-only with external access disabled.

    read_only alone leaves COPY and read_csv working, so both settings are
    needed — see the measurements in the module docstring.
    """
    return duckdb.connect(db_path, read_only=True, config=SAFE_CONFIG)


def validate(sql: str) -> str:
    """
    Return sql if it is exactly one SELECT, else raise GuardrailError.

    Parsed rather than pattern-matched. A query can open with a CTE, hide a
    second statement behind a comment, or bury intent in a subquery, and none
    of that is visible to text matching.
    """
    if not sql or not sql.strip():
        raise GuardrailError("No query to run.")

    try:
        statements = duckdb.extract_statements(sql)
    except Exception:
        # Anything unparseable is refused without echoing the parser's message,
        # which quotes the query back and can carry schema details with it.
        raise GuardrailError("That could not be read as a SQL query.") from None

    if len(statements) != 1:
        raise GuardrailError(
            f"Expected a single query, got {len(statements)}. "
            "Only one statement can be run at a time."
        )

    if statements[0].type != duckdb.StatementType.SELECT:
        raise GuardrailError("Only SELECT queries are allowed here.")

    # Checked against the parser's own rendering of the statement, not the text
    # the model wrote: comments are gone and PRAGMA has already been rewritten
    # into the table function it really is.
    if INTERNALS.search(statements[0].query):
        raise GuardrailError("Only the published tables can be queried.")

    return sql


def run(con: duckdb.DuckDBPyConnection, sql: str,
        limit: int = DEFAULT_LIMIT, timeout: float = DEFAULT_TIMEOUT) -> list[tuple]:
    """
    Validate, cap, and execute with a timeout.

    The query is wrapped in a subquery so the row cap cannot be argued with:
    whatever LIMIT the model wrote, at most `limit` rows come back.
    """
    validate(sql)
    capped = f"SELECT * FROM (\n{sql}\n) AS guarded LIMIT {int(limit)}"

    # DuckDB has no per-query timeout, so a watchdog interrupts the connection
    # from another thread. Without it one runaway query holds the whole API.
    interrupted = threading.Event()

    def stop():
        interrupted.set()
        con.interrupt()

    watchdog = threading.Timer(timeout, stop)
    watchdog.start()
    try:
        return con.execute(capped).fetchall()
    except Exception as exc:
        if interrupted.is_set():
            raise GuardrailError(
                f"That query took longer than {timeout:.0f}s and was stopped. "
                "Try narrowing it down."
            ) from None
        raise GuardrailError(f"That query could not be run: {type(exc).__name__}") from None
    finally:
        watchdog.cancel()
