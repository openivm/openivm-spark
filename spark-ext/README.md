# Spark extension

Spark SQL extension delivering **OpenIVM incremental view maintenance** without
requiring Delta CDF, with opt-in CDF paths where supported.

> Status: under active development.

## Documentation

- [Runtime support](docs/RUNTIME_SUPPORT.md) lists the pinned Spark, Delta, Scala and Java targets and artifact coordinates.
- [Activation](docs/ACTIVATION.md) explains how to load the opt-in extension in Spark.
- [Development](docs/DEVELOPMENT.md) covers the pinned devcontainer, Nx validation and publication workflow.
- [Materialized views](docs/MATERIALIZED_VIEWS.md) documents DDL, refresh strategies, namespaces and experimental CDF paths.
- [Guarded refresh](docs/GUARDED_REFRESH.md) explains query-hash and physical-table identity checks.
- [State synchronization](docs/STATE_SYNCHRONIZATION.md) describes snapshot durability, restore and optional checkpoint optimizations.
- [Generated SQL](docs/GENERATED_SQL.md) describes the native-to-Spark dialect and expression bridge.
- [Standalone streaming tables](docs/STREAMING_TABLES.md) covers streaming sources, managed targets and status polling.
- [Completed execution telemetry](docs/EXECUTION_TELEMETRY.md) defines synchronous export and generated-action diagnostics.
- [Bounded telemetry queue](docs/BOUNDED_TELEMETRY_QUEUE.md) explains the optional best-effort mirror, trace interpretation and health counters.
- [SQL insights](docs/SQL_INSIGHTS.md) documents request-scoped SQL capture and query-hash annotations.
- [SQL log export](docs/QUERY_LOG_EXPORT.md) documents the request-scoped JVM export API.
- [RocksDB window experiments](docs/ROCKSDB_WINDOW_EXPERIMENTS.md) records experimental measurements and their limitations.
- [Darwin experiments](../experiments/darwin/README.md) introduces the trusted state-guard evaluator and maintained lane samples.

## License

See [LICENSE](../LICENSE).
