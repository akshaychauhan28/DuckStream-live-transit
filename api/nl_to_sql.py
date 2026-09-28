"""
Turn a plain-English question into DuckDB SQL.

The model name is never written here. It comes from GROQ_MODEL, because model
lineups move faster than this project does and a name hardcoded today would be
stale within weeks. See docs/DECISIONS.md #5.

The prompt carries the schema and the caveats stored in the database's `about`
table, so the model repeats the project's own qualifications rather than
inventing its own. What it cannot be trusted to do is decide what a fair
punctuality measure is, which is why the table it queries already has delay
computed rather than exposing the raw predictions.

Nothing here is trusted. Whatever comes back goes through guardrails.validate
before it reaches a connection.
"""

import os
import re

import requests

GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"
TIMEOUT = 30

SYSTEM = """You write DuckDB SQL for a database of real Ottawa bus arrivals.

{schema}

What the data means:
{about}

Rules:
- Answer with one SELECT statement and nothing else. No prose, no markdown.
- Query only the `arrivals` table.
- delay_seconds is already computed. Positive is late. Never try to recompute
  it from scheduled_at and arrived_at.
- Prefer median(delay_seconds) over avg, because a few very late buses drag an
  average badly.
- "late", "latest", "worst" and "delayed" all mean large delay_seconds. They
  never mean the most recent arrival time.
- Use route_name and stop_name in results, not the id columns, so answers are
  readable.
- Add a sensible LIMIT when returning rows rather than an aggregate.
- If a question cannot be answered from this table, return:
  SELECT 'cannot answer that from this data' AS answer
"""

FENCE = re.compile(r"^\s*```(?:sql)?\s*|\s*```\s*$", re.IGNORECASE)


class ModelError(Exception):
    """The model could not be reached, or returned nothing usable."""


def describe(con) -> tuple[str, str]:
    """Build the schema and caveat text from the database itself."""
    columns = con.execute("DESCRIBE arrivals").fetchall()
    schema = "Table arrivals, one row per bus arrival at a stop:\n" + "\n".join(
        f"  {name} {kind}" for name, kind, *_ in columns
    )
    notes = con.execute("SELECT topic, detail FROM about").fetchall()
    about = "\n".join(f"  {topic}: {detail}" for topic, detail in notes)
    return schema, about


def clean(text: str) -> str:
    """Strip the markdown fence models add even when told not to."""
    text = FENCE.sub("", text.strip())
    return text.strip().rstrip(";").strip()


def generate_sql(question: str, schema: str, about: str = "") -> str:
    """Ask the model for SQL. The result is unvalidated and untrusted."""
    key = os.environ.get("GROQ_API_KEY", "").strip()
    model = os.environ.get("GROQ_MODEL", "").strip()
    if not key:
        raise ModelError("GROQ_API_KEY is not set.")
    if not model:
        raise ModelError("GROQ_MODEL is not set.")

    try:
        response = requests.post(
            GROQ_URL,
            headers={"Authorization": f"Bearer {key}"},
            json={
                "model": model,
                "temperature": 0,
                "max_tokens": 500,
                "messages": [
                    {"role": "system",
                     "content": SYSTEM.format(schema=schema, about=about)},
                    {"role": "user", "content": question},
                ],
            },
            timeout=TIMEOUT,
        )
    except requests.RequestException as exc:
        raise ModelError(f"Could not reach the model: {type(exc).__name__}") from None

    if response.status_code == 429:
        raise ModelError("The model is rate limited right now. Try again shortly.")
    if response.status_code != 200:
        raise ModelError(f"The model returned HTTP {response.status_code}.")

    try:
        text = response.json()["choices"][0]["message"]["content"]
    except (KeyError, IndexError, ValueError):
        raise ModelError("The model returned an unexpected response.") from None

    sql = clean(text)
    if not sql:
        raise ModelError("The model returned an empty query.")
    return sql
