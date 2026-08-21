"""
Pull and version the static GTFS Schedule bundle.

BLOCKING for Phase 2 — not optional, and not merely cosmetic.

Measured on real payloads: the TripUpdates feed carries `arrival.time` on 100%
of its 14,189 predictions and `arrival.delay` on exactly zero of them. The feed
tells you when a bus is predicted to arrive; it never tells you how late it is.

    delay = predicted_arrival_time (feed) - scheduled_arrival_time (stop_times.txt)

So every delay number in this project comes out of a join against static GTFS.
Without it there is no punctuality analysis, no delay-by-route, no
delay-by-hour — the entire analytical core.

See docs/FEED_NOTES.md and DECISIONS.md #10.

------------------------------------------------------------------------------
Not implemented — Phase 2.
------------------------------------------------------------------------------

WHAT THE JOIN ACTUALLY LOOKS LIKE

`stop_times.txt` has primary key (trip_id, stop_sequence), and GTFS-Realtime's
StopTimeUpdate carries both. So the delay join is exact, not fuzzy:

    rt.trip.trip_id + rt.stop_time_update.stop_sequence
      -> stop_times.(trip_id, stop_sequence).arrival_time

Note that stop_sequence "must increase along the trip but need not be
consecutive" — do not assume 1,2,3.

Minimum files needed: trips.txt, stop_times.txt (the delay join), routes.txt
and stops.txt (names and geography), plus calendar.txt / calendar_dates.txt to
know which service_id is active on a given date.

FOUR TRAPS, ALL FROM THE SPEC

1. Times can exceed 24:00:00. "For times occurring after midnight on the
   service day, enter the time as a value greater than 24:00:00" — so
   `25:35:00` means 1:35 AM the following calendar day. A normal time parser
   will reject or mangle these. Parse as an offset in seconds from midnight of
   the service day, not as a wall clock.

2. stop_times times are in agency.agency_timezone, explicitly NOT
   stops.stop_timezone. The spec calls this out directly so that times increase
   monotonically across a trip even when it crosses timezones. Ottawa is one
   zone so this is easy here, but the conversion has to be deliberate: the RT
   feed gives absolute epoch seconds, static GTFS gives local service time.
   Both sides must reach a common basis before subtracting.

3. Service day is not calendar day. A trip starting at 24:30:00 on Friday runs
   at 00:30 Saturday but belongs to Friday's service. This affects the
   partitioning decision — see DECISIONS.md #7.

4. feed_info.txt carries feed_version, feed_start_date and feed_end_date. That
   is the bundle's own statement of what period it describes, which is exactly
   what the versioned join needs. Prefer it over the download date where it
   exists.

VERSIONING

The bundle is republished periodically and service changes with it. A trip_id
valid in September may not exist in October.

  - Keep every version downloaded, stamped with fetch date AND feed_info values.
  - Never overwrite in place.
  - Join each observation against the bundle version current at the time of the
    observation, not the newest one.

That last point matters more than it looks, because getting it wrong fails
silently. The join still succeeds; the numbers are just quietly wrong.

FETCHING

The spec says datasets should live at a public permanent URL and that servers
should report file modification dates correctly, so Last-Modified / ETag should
be usable to skip unchanged downloads. Verify OC Transpo actually honours that
rather than assuming it.
"""


def main() -> int:
    raise NotImplementedError("Phase 2 — see module docstring.")


if __name__ == "__main__":
    raise SystemExit(main())
