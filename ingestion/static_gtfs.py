"""
Pull and version the static GTFS bundle.

Static GTFS is what turns route_id "1-341" into "Route 95" and makes
scheduled-vs-actual comparison possible. Without it the AI layer cannot answer
a single question a human would actually phrase.

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
