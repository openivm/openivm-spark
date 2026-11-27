# Lanes and ownership

[Darwin experiment](../README.md)

Prefix `CM` = `spark-ext/ivm-common/src/main/scala/org/openivm/spark/common/`, `RDB` = `CM/rocksdb/`,
`MVC` = `spark-ext/ivm-extension/src/main/scala/org/openivm/spark/commands/MaterializedViewCommands.scala`.
`manifest.json` holds the authoritative lists; the evaluator allows owned + shared + `new_files` for the lane and warns
when a shared file is touched (the coordinator integrates overlapping edits). Lane-owned tests stay frozen and are
integrated on the trusted side, never as a scored candidate diff.

| Lane | Goal | Targeted waits (sum over 6 MVs) | Owned | Shared | New files |
| --- | --- | --- | --- | --- | --- |
| A | shared barrier / lock scope | `state_barrier_wait_ms` + `capture_wait_ms` | `RDB/OpenIvmStateBarrier.scala`, `RDB/OpenIvmRocksDBRegistry.scala` | MVC, `CM/MvCatalog.scala` | none |
| B | catalog finalization | `catalog_finalize_ms` | `CM/MvCatalog.scala`, `CM/CdfWatermarkBackend.scala`, `CM/StagingCatalog.scala`, `CM/DeltaPathTable.scala` | MVC, `CM/DeltaMvCatalogBackend.scala` | none |
| C | per-MV publication / queue | `publication_queue_wait_ms` + `publication_ms` | `RDB/OpenIvmStateSync.scala` | MVC, `RDB/OpenIvmStateBarrier.scala` | none |
| D | whole-path efficiency + catalog backend | `wall_ms` (catalog `fs_ops` total informational) | `CM/MvCatalogBackend.scala`, `CM/FeatureGate.scala` | MVC, `CM/DeltaMvCatalogBackend.scala`, `CM/MvCatalog.scala`, `CM/DeltaPathTable.scala` | none |
| E | incremental state shipping | `counts.state_sync.bytes_uploaded` (state-sync write ops informational) | none | `RDB/OpenIvmStateSync.scala` | `RDB/OpenIvmStateDelta*.scala`, `RDB/OpenIvmStateSyncCodec*.scala` |

Frozen for every lane (any diff vs the baseline => 0.0): `experiments/**`, `spark-ext/**/src/test/**`,
`spark-ext/build.sbt`, `spark-ext/project/**`, `spark-ext/dev/**`, `spark-ext/project.json`, `.github/**`, `project.json`,
`nx.json`, `package.json`, `package-lock.json`, `.devcontainer/**`, `contrib/**`, telemetry/measurement files
(`**/telemetry/**`, `OpenIvmExecutionSpan.scala`, `RefreshProfile.scala`, `PhaseTimeline.scala`) and any `*Perf*Spec*`,
`state-guard*` or `.gitattributes` path. Also rejected: deleted files, files outside the lane allowlist, symlinks, binary
blobs and file-mode changes. The changed set is `git diff --name-status --no-renames <baseline>` plus
`git ls-files --others --exclude-standard`; Darwin's `output/*.json|md` protocol files are ignored.

Use the [running guide](RUNNING_LANES.md) for the five maintained configs and trusted evaluator setup.
