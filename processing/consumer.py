"""
Polars streaming consumer: dedupe, derive fields, join static GTFS.

    topic: vehicle-positions  ->  clean records ready for storage/writer.py

------------------------------------------------------------------------------
Not implemented — Phase 2.
------------------------------------------------------------------------------

Design questions:

  1. Dedupe key. GTFS-RT vehicle timestamps only advance when a vehicle actually
     reports. Polling every 30s against vehicles reporting every 20-60s means a
     large share of rows are exact repeats. Measure that share on real data
     before choosing — the reduction is worth measuring.

  2. Speed between GPS points. Some feeds populate entity.vehicle.position.speed
     directly; some don't, and some populate it with garbage. Check your real
     fixtures before deciding whether to trust it or derive it from consecutive
     positions. If you derive it: what do you do when a vehicle's GPS jumps
     500m in 2 seconds? Dropping vs. clamping vs. flagging is a real choice and
     it changes your delay statistics.

  3. Ordering. Derived speed requires the previous position for the same
     vehicle. Does your consumer hold per-vehicle state, or do you compute this
     later in DuckDB with a window function over the archive? The second is
     simpler and replay-safe. Think about which before writing state.

  4. Static GTFS join. Do you join here (records land human-readable) or at
     query time in DuckDB (archive stays raw, joins stay flexible)? Joining
     late is usually right for a data lake, but argue it either way — just be
     able to say why.

  5. Schema. Pin an explicit Polars schema rather than letting it infer.
     Inference across days of live data is what produces union_by_name pain
     later. Write it down once you've seen real fixtures — not before.
"""


def main() -> int:
    raise NotImplementedError("Phase 2 — see module docstring.")


if __name__ == "__main__":
    raise SystemExit(main())
