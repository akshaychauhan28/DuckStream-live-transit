"""
Natural language -> DuckDB SQL via Groq.

------------------------------------------------------------------------------
Not implemented — Phase 4.
------------------------------------------------------------------------------

Hard rule: the model name comes from os.environ["GROQ_MODEL"] and appears
nowhere else — not in a default argument, not in a docstring, not in the README.
See docs/DECISIONS.md #5. Pick the current fast/cheap Groq model at build time.

Design questions:

  1. Schema in the prompt. The model needs to know your columns. Do you paste
     the full schema every call (costs tokens, always accurate) or a curated
     summary (cheaper, can drift)? What happens when the schema changes?

  2. Groq free tier is limited per minute and per day. A public demo link can
     exhaust it in an afternoon. Cache aggressively — many visitors will ask
     near-identical questions, and a cache hit is both free and instant.

  3. Generated SQL goes to guardrails.validate() before it goes anywhere near a
     connection. No exceptions, no "just for testing" bypass.

  4. Failure modes worth handling: the model returns prose instead of SQL,
     returns SQL in a markdown fence, invents a column, or writes valid SQL
     that answers a different question than the one asked. The last is the
     hardest and the most interesting to write about.

  5. Prove the model abstraction: run the same prompt set against two different
     Groq models. If swapping is a one-line change, you've demonstrated it
     rather than claimed it.
"""


def generate_sql(question: str, schema: str) -> str:
    """Return candidate SQL for `question`. Must be validated before execution."""
    raise NotImplementedError("Phase 4 — see module docstring.")
