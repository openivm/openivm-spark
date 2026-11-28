# Report schema

[Darwin experiment](../README.md)

`state-guard-perf.json` (`openivm.state-guard-perf/v1`), all numbers finite and `>= 0`; an unknown `evidence_class` is invalid:

```
{ "schema": "openivm.state-guard-perf/v1", "evidence_class": "local-relative-modeled-latency",
  "spark_target": "spark-3.5|spark-4.1", "head_sha": str,
  "latency_profile": {"name", "catalog_read_ms", "catalog_write_ms", "state_read_ms", "state_write_ms", "state_rename_ms", "scale"},
  "end_to_end_ms": num,            // must equal makespan_ms
  "makespan_ms": num, "drain_ms": num, "serial_sum_ms": num, "max_concurrent_engine": int,
  "engine_overlap_ms": num, "barrier_hold_overlap_ms": num,
  "views": [ // exactly 6, mv_index 1..6
    {"mv_index", "name", "wall_ms", "refresh_type", "outcome", "submit_to_ack_ms",
     "state_barrier_wait_ms", "state_barrier_hold_ms", "capture_wait_ms", "catalog_finalize_ms",
     "publication_queue_wait_ms", "publication_ms", "engine_exec_ms",
     "submit_at_ms", "ack_at_ms", "barrier_acquired_at_ms", "barrier_released_at_ms",
     "engine_start_at_ms", "engine_end_at_ms", "catalog_finalize_start_at_ms", "catalog_finalize_end_at_ms",
     "publication_start_at_ms", "publication_end_at_ms"} ],   // *_at_ms: offsets from batch start
  "counts": {"incremental_count", "full_refresh_count",
     "fs_ops": {"state_sync": {"read", "write", "rename", "delete", "list"}, "catalog": {same}},
     "state_sync": {"publications", "commits", "snapshots", "bytes_uploaded"}},
  "gates": {"parity": bool, "incremental_only": bool, "state_sync_consistent": bool},
  "notes": [str] }
```

`state-guard-crash.json` (`openivm.state-guard-crash/v1`):
`{"points": [{"name", "mode", "crashed", "recovered", "parity", "metadata_converged", "incremental_after_recovery", "known_gap", "notes"}]}`.
`evaluator/report.py` is the validator and the exact contract. Beyond field types it checks that
`end_to_end_ms == makespan_ms`, `serial_sum_ms == sum(submit_to_ack_ms)`, `mv_index` is exactly 1..6, each interval's end
does not precede its start, `submit_to_ack_ms` agrees with `ack_at_ms - submit_at_ms`, the makespan does not end before
the last ack, and `max_concurrent_engine` matches the per-view engine offsets.

## Synthetic report sets

The [offline generator](../fixtures/make_fixtures.py) materializes
`.scratch/fixtures/reports/<set>/state-guard-perf.json`,
`state-guard-crash.json` and `run.json` under `experiments/darwin`.
`run.json` carries suite `exit_codes`, failure lines and a JUnit `outcomes`
map of `"<suite>: <test>"` to `passed|failed|skipped`.
Optional `changes.json` contains candidate changes as
`[{"path", "status", "old_mode", "new_mode", "binary"}]`; its absence means
no changes. These files are synthetic, disposable test inputs, never actual
measurement reports. See [offline validation](OFFLINE_VALIDATION.md) to generate
them and exercise every negative gate.
