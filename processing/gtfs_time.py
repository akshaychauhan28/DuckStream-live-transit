"""
Turn a scheduled GTFS time into an absolute instant.

Not implemented — Phase 2.

The realtime feed gives predicted arrivals as Unix timestamps. The timetable
gives scheduled arrivals as text like "17:40:00", in Ottawa local time, counted
from the start of a *service day*. Delay is the difference between them, so one
has to be converted into the other's terms first.

    delay_seconds = predicted_epoch - scheduled_epoch(start_date, arrival_time)

THREE THINGS MAKE THIS HARDER THAN IT LOOKS

1. Times can exceed 24:00:00.

   stop_times.txt really contains values like "25:35:00", meaning 1:35am on the
   following calendar day, still belonging to the previous service day. A bus
   leaving at 23:50 and arriving at 00:20 is one continuous trip, and GTFS keeps
   its times increasing rather than wrapping around. Any clock-time parser
   rejects these.

   The spec also permits a single-digit hour, so "7:05:00" is valid.

2. The two sides use different clocks.

   "17:40:00" is a wall clock in Ottawa. 1789674000 is seconds since 1970 UTC.
   Converting needs the service date as well as the time.

3. A service day is not 24 hours on the days clocks change.

   The spec defines these times as measured from "noon minus 12h" of the service
   day — "effectively midnight except for days on which daylight savings time
   changes occur".

   That wording is doing real work. On 2026-11-01, when clocks go back, noon
   minus twelve hours lands at 01:00 local, not midnight, because the day has 25
   hours. Anchoring at noon and subtracting a fixed twelve hours is what keeps
   the arithmetic well defined on both the 23- and 25-hour days.

   Consequence worth knowing: on a clock-change day, service times before the
   transition land an hour away from their wall-clock reading. That is what the
   spec prescribes, and it is invisible until Nov 1 — after this project's
   window — but the behaviour is tested so it can't surprise anyone later.

CONTRACT

    parse_gtfs_time("17:40:00")   -> 63600     seconds from the service-day base
    parse_gtfs_time("25:35:00")   -> 92100     past midnight, not an error
    parse_gtfs_time("7:05:00")    -> 25500     single-digit hour is legal

    scheduled_epoch("20260917", "17:40:00") -> 1789674000
        service_date is YYYYMMDD, as it appears in both the timetable and the
        realtime feed's start_date.

NOTES

    from zoneinfo import ZoneInfo
    OTTAWA = ZoneInfo("America/Toronto")

    noon  = datetime(y, m, d, 12, tzinfo=OTTAWA)
    base  = noon.astimezone(timezone.utc) - timedelta(hours=12)
    when  = base + timedelta(seconds=parse_gtfs_time(text))
    return int(when.timestamp())

    Converting to UTC before subtracting the twelve hours matters: it makes
    `base` a fixed instant, so adding seconds is real elapsed time rather than
    wall-clock arithmetic that DST can bend.

    zoneinfo needs the `tzdata` package on Windows — the OS ships no timezone
    database of its own.

Run the tests to check your work:

    .venv\\Scripts\\python.exe -m pytest tests/test_gtfs_time.py -v
"""

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

OTTAWA = ZoneInfo("America/Toronto")


def parse_gtfs_time(text: str) -> int:
    """
    Seconds from the start of the service day.

    Deliberately does not treat the hour as a clock value. "25:35:00" is 1:35am
    the next calendar day and must stay a larger number, not wrap to 01:35 —
    that is what keeps a trip's times increasing across midnight.
    """
    hours, minutes, seconds = text.split(":")
    return int(hours) * 3600 + int(minutes) * 60 + int(seconds)


def scheduled_epoch(service_date: str, gtfs_time: str, tz: ZoneInfo = OTTAWA) -> int:
    """
    Absolute Unix timestamp for a scheduled time on a given service date.

    service_date is YYYYMMDD, matching both the timetable's calendar files and
    the realtime feed's start_date.
    """
    year = int(service_date[0:4])
    month = int(service_date[4:6])
    day = int(service_date[6:8])

    # The spec measures service times from "noon minus 12h" of the service day,
    # which is midnight on ordinary days and deliberately isn't on the two days
    # a year when clocks change.
    #
    # Converting to UTC *before* subtracting is the part that matters: it makes
    # `base` a fixed instant, so adding seconds is real elapsed time. Subtracting
    # while still in Ottawa time would be wall-clock arithmetic, which daylight
    # saving bends — correct every day except the one day it isn't.
    noon = datetime(year, month, day, 12, tzinfo=tz)
    base = noon.astimezone(timezone.utc) - timedelta(hours=12)

    when = base + timedelta(seconds=parse_gtfs_time(gtfs_time))
    return int(when.timestamp())
