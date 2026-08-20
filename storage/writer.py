"""
Flush buffered records to partitioned Parquet.

------------------------------------------------------------------------------
Not implemented — Phase 2. The partitioning strategy is an open decision;
see docs/DECISIONS.md #7. It gets settled by measurement, not by an
assumption baked in here.
------------------------------------------------------------------------------

Phase 2 deliverable is a bake-off, not an implementation:

    A) partition by date only, rows sorted by route/vehicle within each file
    B) partition by date AND route

Build both, run the same queries against both on real accumulated data, and
record: query time, file count, total size on disk, and how each behaves on the
HDD vs. after the SSD upgrade.

Prior expectation is that B loses badly (~120 routes x 30 days = 3,600+
directories of small files) — but that needs to be measured, not assumed.

Things worth measuring while you're in here:
  - row group size, and whether DuckDB's statistics actually prune on your
    sort column (check with EXPLAIN ANALYZE, don't assume)
  - compression codec: zstd vs snappy, on both size and read speed
  - flush interval: how small do files get before the small-file cost shows up
"""


def main() -> int:
    raise NotImplementedError("Phase 2 — see module docstring.")


if __name__ == "__main__":
    raise SystemExit(main())
