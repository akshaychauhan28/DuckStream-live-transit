"""
Tests for picking which timetable version applies to a given day.

This is the piece that stops a delay figure being computed against the wrong
schedule. That failure is silent — the join succeeds and the numbers look
ordinary — so the rule is tested rather than trusted.
"""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "ingestion"))

from static_gtfs import bundle_for  # noqa: E402

SEPTEMBER = {
    "feed_version": "S1000517",
    "feed_start_date": "20260910",
    "feed_end_date": "20261010",
    "first_fetched_at": "2026-09-17T19:00:00+00:00",
}
OCTOBER = {
    "feed_version": "S1000518",
    "feed_start_date": "20261010",
    "feed_end_date": "20261110",
    "first_fetched_at": "2026-10-11T08:00:00+00:00",
}
MANIFEST = {"versions": [SEPTEMBER, OCTOBER]}


def test_picks_the_bundle_covering_the_day():
    assert bundle_for("20260915", MANIFEST)["feed_version"] == "S1000517"
    assert bundle_for("20261101", MANIFEST)["feed_version"] == "S1000518"


def test_accepts_several_date_formats():
    import datetime

    for value in ("20260915", "2026-09-15", datetime.date(2026, 9, 15)):
        assert bundle_for(value, MANIFEST)["feed_version"] == "S1000517"


def test_covering_bundle_is_not_marked_approximate():
    assert bundle_for("20260915", MANIFEST)["approximate"] is False


def test_boundaries_are_inclusive():
    assert bundle_for("20260910", MANIFEST)["feed_version"] == "S1000517"
    assert bundle_for("20261110", MANIFEST)["feed_version"] == "S1000518"


def test_overlap_day_prefers_the_newer_publication():
    """Both bundles claim 2026-10-10; the one published later was in force."""
    assert bundle_for("20261010", MANIFEST)["feed_version"] == "S1000518"


def test_refuses_when_nothing_covers_the_day():
    """
    The whole point. September data joined to an October timetable produces
    delays that are wrong and look fine, so this must fail loudly instead.
    """
    with pytest.raises(LookupError) as excinfo:
        bundle_for("20260801", MANIFEST)

    message = str(excinfo.value)
    assert "2026-08-01" in message
    assert "S1000517" in message, "the error should say what is held"
    assert "allow_approximate" in message, "the error should say what to do"


def test_refuses_for_a_day_after_everything_held():
    with pytest.raises(LookupError):
        bundle_for("20270101", MANIFEST)


def test_approximate_falls_back_to_the_nearest_earlier_bundle():
    result = bundle_for("20261201", MANIFEST, allow_approximate=True)
    assert result["feed_version"] == "S1000518"
    assert result["approximate"] is True, "looseness must travel with the result"


def test_approximate_still_refuses_when_nothing_is_early_enough():
    """Falling back to a *later* timetable would be the exact mistake."""
    with pytest.raises(LookupError):
        bundle_for("20260801", MANIFEST, allow_approximate=True)


def test_empty_manifest_refuses_and_says_so():
    with pytest.raises(LookupError) as excinfo:
        bundle_for("20260915", {"versions": []})
    assert "none" in str(excinfo.value)
