# Generated SQL dialect

The Spark SQL generation boundary translates DuckDB pattern operators `~~`,
`!~~`, `~~*` and `!~~*` to LIKE, NOT LIKE, ILIKE and NOT ILIKE. String literals,
quoted identifiers and comments are protected; source business SQL is unchanged.
The native infinity literal `1e1000` is restored as a Spark DOUBLE infinity;
the native `main.trim` builtin is restored as an unqualified Spark `trim` call.

Native compilation preserves semicolons inside string literals and quoted
identifiers. Initial-load statement extraction recognizes terminators only
outside quoted regions and comments, preserving literal values through CREATE
and incremental REFRESH.

For native binding failures, unqualified-column `UNIX_TIMESTAMP`, boolean `LOWER`, and
`TIMESTAMPDIFF(SECOND, ...)` calls are retried through a Spark-evaluated bridge.
Other argument shapes are not covered by this retry.

The expression bridge preserves Spark evaluation for supported expressions
while the native compiler classifies the view. Daily `EXPLODE(SEQUENCE(...))`
with DATE column bounds and `INTERVAL 1 DAY` is supported, including multiple
generators. Struct ordering keys are bridged only for bounded `MAX_BY` shapes;
this is not a general translation of Spark struct ordering to DuckDB.

See [materialized views](MATERIALIZED_VIEWS.md) for refresh strategies and
[generated action diagnostics](EXECUTION_TELEMETRY.md#generated-action-diagnostics)
for compiler-source attribution limits.
