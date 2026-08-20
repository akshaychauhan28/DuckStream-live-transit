"""
FastAPI app for the public natural-language query demo.

------------------------------------------------------------------------------
Not implemented — Phase 4.
------------------------------------------------------------------------------

Flow:
    question -> nl_to_sql.generate_sql() -> guardrails.validate() -> DuckDB -> answer

Deployment target is Hugging Face Spaces, against a trimmed subset of the
archive rather than the full lake.

Consider starting this skeleton during Phase 3 while data accumulates —
deployment reliably eats more time than budgeted, and Phase 4 is the tightest
window in the plan.
"""
