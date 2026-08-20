"""
Pull and version the static GTFS bundle.

BLOCKING for Phase 2 — not optional, and not merely cosmetic.

Measured on real payloads: the TripUpdates feed carries `arrival.time` on 100%
of its 14,189 predictions and `arrival.delay` on exactly zero of them. The feed
tells you when a bus is predicted to arrive; it never tells you how late it is.

    delay = predicted_arrival_time (feed) - scheduled_arrival_time (stop_times.txt)

So every delay number in this project comes out of a join against static GTFS.
Without it there is no punctuality analysis, no delay-by-route, no
delay-by-hour — the entire analytical core. It also turns route_id "23" into
"Route 23" for the AI layer, but that's the smaller half of why it matters.

See docs/FEED_NOTES.md and DECISIONS.md #10.

------------------------------------------------------------------------------
Not implemented — Phase 2.
------------------------------------------------------------------------------

The part that makes this non-trivial: the bundle is republished periodically,
and service changes with it. A trip_id valid in September may not exist in
October.

So this is a slowly-changing dimension, not a static file:
  - Keep every version you download, stamped with the date you fetched it.
  - Never overwrite in place.
  - When joining live records to the bundle, join against the version that was
    current *at the time of the record* — not the newest one. Otherwise your
    September data gets interpreted with October's schedule.

That last point matters more than it looks, because getting it wrong fails
silently. The join still succeeds; the numbers are just quietly wrong.

Find the bundle URL on OC Transpo's developer page. Check whether it exposes
Last-Modified or ETag so you can skip re-downloading unchanged bundles.
"""


def main() -> int:
    raise NotImplementedError("Phase 2 — see module docstring.")


if __name__ == "__main__":
    raise SystemExit(main())
