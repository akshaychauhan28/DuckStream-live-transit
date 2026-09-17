"""
Tests for converting scheduled GTFS times into absolute instants.

Every delay figure in the project passes through this, and a mistake here
shifts delays by an hour or a day while still looking completely ordinary.
"""

import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "processing"))

from gtfs_time import parse_gtfs_time, scheduled_epoch  # noqa: E402

OTTAWA = ZoneInfo("America/Toronto")


def local(epoch: int) -> datetime:
    """Render an instant as Ottawa wall-clock, for readable assertions."""
    return datetime.fromtimestamp(epoch, OTTAWA)


# --------------------------------------------------------------------------
# parsing
# --------------------------------------------------------------------------

@pytest.mark.parametrize("text,expected", [
    ("00:00:00", 0),
    ("00:00:30", 30),
    ("01:00:00", 3600),
    ("17:40:00", 63600),
    ("23:59:59", 86399),
])
def test_parses_ordinary_times(text, expected):
    assert parse_gtfs_time(text) == expected


@pytest.mark.parametrize("text,expected", [
    ("24:00:00", 86400),
    ("25:35:00", 92100),
    ("27:15:30", 98130),
])
def test_parses_times_past_midnight(text, expected):
    """A bus leaving at 23:50 and arriving at 00:20 keeps counting upward."""
    assert parse_gtfs_time(text) == expected


def test_accepts_single_digit_hour():
    """The spec allows H:MM:SS as well as HH:MM:SS."""
    assert parse_gtfs_time("7:05:00") == 25500


@pytest.mark.parametrize("text", ["", "nonsense", "17:40", "17-40-00", None])
def test_rejects_what_it_cannot_parse(text):
    with pytest.raises((ValueError, TypeError, AttributeError)):
        parse_gtfs_time(text)


# --------------------------------------------------------------------------
# conversion to an instant
# --------------------------------------------------------------------------

def test_ordinary_day_matches_ottawa_wall_clock():
    epoch = scheduled_epoch("20260917", "17:40:00")
    assert local(epoch).strftime("%Y-%m-%d %H:%M") == "2026-09-17 17:40"


def test_midnight_is_the_start_of_the_service_day():
    epoch = scheduled_epoch("20260917", "00:00:00")
    assert local(epoch).strftime("%Y-%m-%d %H:%M") == "2026-09-17 00:00"


def test_past_midnight_lands_on_the_next_calendar_day():
    """25:35 on the 17th is 1:35am on the 18th, still the 17th's service day."""
    epoch = scheduled_epoch("20260917", "25:35:00")
    assert local(epoch).strftime("%Y-%m-%d %H:%M") == "2026-09-18 01:35"


def test_later_times_produce_later_instants():
    times = ["06:00:00", "12:00:00", "23:00:00", "24:30:00", "26:00:00"]
    epochs = [scheduled_epoch("20260917", t) for t in times]
    assert epochs == sorted(epochs), "times within a service day must increase"


def test_result_is_an_integer_epoch():
    epoch = scheduled_epoch("20260917", "17:40:00")
    assert isinstance(epoch, int)
    assert 1_600_000_000 < epoch < 2_000_000_000


def test_matches_a_hand_checked_utc_instant():
    """17:40 in Ottawa on 2026-09-17 is 21:40 UTC — EDT is UTC-4 in September."""
    expected = int(datetime(2026, 9, 17, 21, 40, tzinfo=timezone.utc).timestamp())
    assert scheduled_epoch("20260917", "17:40:00") == expected


# --------------------------------------------------------------------------
# the days clocks change
# --------------------------------------------------------------------------

def test_service_day_base_on_a_25_hour_day():
    """
    2026-11-01, clocks go back. Noon minus twelve hours lands at 01:00 local,
    not midnight, because the day is 25 hours long. That is what the spec's
    "noon minus 12h" wording exists to define.
    """
    epoch = scheduled_epoch("20261101", "00:00:00")
    assert local(epoch).strftime("%Y-%m-%d %H:%M") == "2026-11-01 01:00"


def test_a_25_hour_day_still_advances_evenly():
    """Every hour of service time is a real hour of elapsed time."""
    start = scheduled_epoch("20261101", "00:00:00")
    for hours in range(1, 12):
        assert scheduled_epoch("20261101", f"{hours:02d}:00:00") == start + hours * 3600


def test_afternoon_on_a_clock_change_day_reads_correctly():
    """After the transition, wall clock and service time agree again."""
    epoch = scheduled_epoch("20261101", "14:00:00")
    assert local(epoch).strftime("%H:%M") == "14:00"


def test_spring_forward_day_advances_evenly_too():
    """2027-03-14 is 23 hours long; elapsed seconds stay real seconds."""
    start = scheduled_epoch("20270314", "00:00:00")
    assert scheduled_epoch("20270314", "10:00:00") == start + 10 * 3600
