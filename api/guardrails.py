"""
SQL execution guardrail for the public natural-language endpoint.

------------------------------------------------------------------------------
Not implemented — Phase 4. Required before any public deployment.
------------------------------------------------------------------------------

Threat model, stated plainly: this endpoint executes SQL written by a language
model, on a public URL, against DuckDB. DuckDB can write files (COPY ... TO) and
load extensions (INSTALL / LOAD). Unrestricted execution of generated SQL is
therefore closer to remote code execution than to a typo problem. DuckDB's own
security documentation says to treat SQL from untrusted sources as untrusted
code.

Required before anything is deployed publicly (spec section 7):

  1. Open the connection read-only.
  2. SET enable_external_access = false
  3. Parse the generated SQL; reject anything that is not a single SELECT.
     Note: string matching on "SELECT" is not parsing. A statement can begin
     with a CTE, contain multiple statements separated by semicolons, or hide
     intent in a subquery. Use duckdb.extract_statements() or an equivalent so
     you are reasoning about parsed structure, not text.
  4. Force a LIMIT regardless of what the model produced.
  5. Enforce a query timeout.
  6. Rate-limit the endpoint — protects against both abuse and burning the
     Groq free tier in an afternoon.
  7. Cache repeated questions rather than re-querying the model each time.

Worth thinking through:
  - Defence in depth: if your parser has a bug, what is the second thing that
    stops the query? A read-only connection over a copy of the data means a
    parser bypass still can't damage the archive.
  - What does the user see when a query is rejected? A useful error without
    leaking your schema or internals.
  - Write the tests adversarially. Try to defeat the validator with stacked
    statements, CTEs that wrap a COPY, comment-obfuscated keywords, and unicode
    tricks. Anything that gets through is a bug to fix before deploying.
"""


def validate(sql: str) -> str:
    """Return safe-to-execute SQL, or raise."""
    raise NotImplementedError("Phase 4 — see module docstring.")
