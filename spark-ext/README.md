# Spark extension

## Generated action diagnostics

`spark.openivm.refresh.generatedActionOwnership.enabled=true` adds
`openivm.generated.action` to Spark job properties within supported generated
SQL dispatches. Join by `refresh_id` and `action_id`; `boundary=sql` is the
potentially eager SQL call and `boundary=collect` is the result action.
Compiler/source and execution digests identify the statement, not its stages.
A simple-projection bag INSERT retains its compiler source when its net-view
input reads fused scratch; an atomic apply combining outputs is not assigned a single source.
Nested Delta log/state jobs may inherit this property, and deferred work on
other threads may lack it. Helpers without a proven source mapping are
untagged. This diagnostic is off by default and assigns no execution-time credit.

## Experimental global-window cascades

`spark.openivm.refresh.globalWindowCdfMerge.enabled` is opt-in and defaults to
`false`. For a global-window recompute with a downstream consumer, actual CDF
propagation, and a CDF-enabled physical target, it replaces the full target in
one Delta MERGE. This produces CDF deletes for the old rows and inserts for the
new rows; the downstream view can consume them incrementally. The full-domain
rewrite is an executor helper, not generated incremental SQL. It can write a
large change feed and has not been shown to improve end-to-end performance.
Unscoped external overwrites and unavailable change history keep their normal
full-refresh safeguards.

## Guarded refresh

When the extension and feature gate are enabled, `SET spark.openivm.capabilities`
advertises `ensure_refresh_v2`. Clients can opt in using

```sql
REFRESH MATERIALIZED VIEW db.view EXPECT QUERY HASH '0123456789abcdef0123456789abcdef' IF EXISTS
```

The 32-character MD5 is the existing
`dbt.fabricspark.materialized_view.query_hash` property, calculated by the client
from its exact UTF-8 compiled SQL. Under the refresh locks, OpenIVM checks the
stored and live properties and validates the live target ownership before
running the ordinary refresh. A mismatch or invalid target fails; only a name
with neither a registered MV nor a live relation returns `absent`.
Guarded refresh also requires the physical Delta table ID recorded by CREATE;
legacy MVs without this identity fail closed and remain usable through the
ordinary refresh path.

The single result row contains `status`, `query_hash`, `target`,
`target_table_id`, `refresh_id`, and `outcome`. `target` is the input identifier
rendered with each component backtick-quoted and escaped; it is not resolved to
a default namespace. One to three identifier components are supported; four or
more are rejected. The final three fields are null for `absent`. A guarded
refresh of a registered MV requires `spark.openivm.stateSync.async.enabled=false`
and fails before writing otherwise. When a state-sync URI is configured, a
`refreshed` result follows synchronous publication; ordinary asynchronous
refreshes do not imply remote publication has completed. Older JARs do not advertise
the capability and clients must retain their normal fallback.

The loaded engine also advertises `ensure_refresh_eligibility_v1`. Before a
strict guard, clients may run:

```sql
SHOW OPENIVM REFRESH ELIGIBILITY db.view EXPECT QUERY HASH '0123456789abcdef0123456789abcdef'
```

The single row contains `status`, `query_hash`, `target`, `target_table_id`,
and `reason`. `eligible` has a verified physical ID and null reason;
`lifecycle_required` has a null ID and one of `registration_missing`,
`legacy_identity_missing`, `query_hash_policy`, `public_relation_missing`,
or `async_unsupported`. The command does not refresh or publish state. Unsafe
ownership, mismatched physical identity, and validation failures are errors.
Eligibility is not a refresh result: a later strict guard revalidates and any
subsequent failure must not trigger fallback.
The strict v2 command does not accept the probed table ID as an input: a
managed drop/recreate can validate a new identity, and a drop can yield
`absent`. Comparing the strict result's identity to the probe after return
detects replacement but cannot prevent writing to the replacement.

When the loaded engine advertises `ensure_refresh_table_id_v1`, callers can
pin the eligible physical ID before refresh writes:

```sql
REFRESH MATERIALIZED VIEW `lakehouse`.`schema`.`view`
EXPECT QUERY HASH '0123456789abcdef0123456789abcdef'
EXPECT TABLE ID '01234567-89ab-cdef-0123-456789abcdef' IF EXISTS
```

The pinned form fails if the target is absent or its validated live and
persisted ID differs from the pin. It does not fall back or recreate the view.
Failure telemetry may be recorded, but the rejected refresh does not advance
the target or source state or publish a checkpoint. The unpinned v2 form
retains its original behavior. The pin check is protected against concurrent
managed operations sharing the same resolved metadata lock key in one driver;
it is not an alias-based or distributed lock.

## Crash-safe state-sync snapshots

Synchronous backups retain per-namespace request barriers: only requests captured
before a pass receive its outcome. Later requests require a follow-up pass;
covered failures propagate and a later explicit request can retry. Fair pass
admission prevents asynchronous work overtaking waiting synchronous requests.

New generations pack the complete state tree into one immutable `snapshot.zip`
under `.openivm-state-sync/snapshots/<generation>/`. A local capture barrier
drains in-flight state mutations and checkpoints each RocksDB shard, including
its OpenIVM transaction manifests, as one coherent generation. Compression
and upload run after the barrier is released, so later mutations can proceed.
Each generation uses one data-object upload regardless of file count.
No synchronous request succeeds before upload/close, validation and atomic
commit publication.

The capture barrier coordinates threads within one driver JVM. Existing
per-database multi-process locking remains in force, but the barrier does not
provide an atomic cross-process cut or order publishers in different processes.

The closed `OPENIVM_STATE_SNAPSHOT_V2` completion record contains generation,
archive length and SHA256; same-filesystem rename publishes it under
`.openivm-state-sync/commits/`. Restore verifies archive length/checksum and bounded
safe ZIP entries/CRC in an isolated local staging directory before atomically
publishing local state. Corrupt committed snapshots fail closed, without falling
back to older or empty state. Incomplete generations are ignored.

V1 committed tree snapshots and legacy flat snapshots remain readable. Older
readers cannot read V2: downgrades must not silently reuse V2 state. Old
generations are pruned according to the configured retention. Archives are bounded to
100,000 entries and 64 GiB compressed/expanded bytes; links and unsafe paths are
rejected.

### Guarded SQL checkpoints (opt-in)

For an externally exclusive single writer, configure
`spark.openivm.stateSync.guarded.uri` at Spark startup and call
`GuardedCheckpointDriver.attachSqlPersistence(spark, storageCredential)` on
the driver before accessing OpenIVM. Use a pre-provisioned guarded V1
namespace and an initially empty local state root. Do not configure
`spark.openivm.stateSync.uri`; explicitly set
`spark.openivm.stateSync.async.enabled=false` and `spark.openivm.gc.enabled=false`.
The route refuses legacy fallback or the legacy collector's unguarded deletes.

First OpenIVM access restores only the latest verified published archive.
Ordinary CREATE and REFRESH return their existing SQL responses after a
covering publication has been confirmed. Failed publications do not return
success. This does not change archive retention or enable pruning. The
per-generation terminal decision is not a namespace writer fence: independent
drivers must not write the same namespace concurrently. The qualification-only
`publish`, `restore`, and `retire` methods remain separate.

The opt-in synchronous checkpoint canary is disabled by default:
`spark.openivm.stateSync.checkpoint.enabled=true`. It reuses only a committed V2
archive whose captured mutation generation covers the new request. Eligibility
requires an explicit local `spark.openivm.statePath` disjoint from the Spark
warehouse, single-process RocksDB, no telemetry file sinks, and a destination URI
not observed with a second local state root in this driver. Any unresolved/shared
configuration takes a fresh synchronous capture on every request; it never reuses
an in-flight pass or cached archive. Requests with state-sync telemetry enabled
also take the ordinary traced publication path. Opt-in receipts bind acknowledgements to the
committed generation, captured/requested mutation generations, archive SHA256 and
byte count. Async backups retain their existing behavior and invalidate synchronous
reuse proof. The URI must still have a single writer
across JVMs: another process using the same destination cannot be detected by the
in-process root check. This opt-in changes neither the V2 archive nor restore format.

`spark.openivm.stateSync.parallelCapture.enabled=true` is a separate, default-off
option for single-process RocksDB. It checkpoints independent shards on at most
four workers while the publisher holds the same root capture barrier. Shard
handles retain their existing locks, and the publisher joins all workers before
packing or releasing the barrier, including after errors. Multi-process mode
continues to capture serially. Publication, acknowledgement and restore are unchanged.

`spark.openivm.cdf.preflight.parallelSources.enabled=true` enables an experimental,
default-off source-availability preflight using at most two workers per refresh.
It checks independent source ranges before applying refresh SQL; unavailable or
missing-baseline sources retain the existing recovery decision. It does not
parallelize refresh SQL or change publication and acknowledgment ordering.

The experimental `spark.openivm.stateSync.checkpointReuse.enabled` setting defaults
to `false`. When enabled, eligible unchanged, live single-process stores can reuse
verified in-memory checkpoint bytes; other stores use the normal native checkpoint.
For legacy V2 publication, each backup still publishes an independently
restorable generation. Guarded SQL checkpoints take a fresh store capture.
The cache
retains at most 128 MiB of file bytes across 256 stores, with at most 16 MiB
and 1,024 files per store. Successful
generation logs report `fresh`, `reuse`, or `fallback` per hashed store path;
failed publication logs are reported separately.

## Cross-thread query-hash annotations

When an event callback began capture on a different Java thread, append
`FOR REQUEST '<existing-request-id>'` to `OPENIVM INSIGHTS ANNOTATE MATERIALIZED VIEW
QUERY HASH ...`. The exact application-scoped request must already be active and
have matching materialized-view model context. Missing, ended, mismatched, or
conflicting bound requests fail closed. No new request, global/session binding or
thread-local rebinding occurs; the lifecycle owner still ENDs, exports and RELEASEs
the original capture. Omitting FOR REQUEST retains thread-bound behavior.

## Generated SQL dialect

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

## Native streaming status lookup diagnostics

Targeted SHOW analyzes the exact target without suppressing native catalog/storage
exceptions. A known-owned target whose lookup fails remains a failure row:
`last_failure` includes the bounded, redacted exception/cause chain. Ownership is
not assumed from registry state; no path fallback, retry or successful-completion
substitution is performed.

## Managed initial CTAS namespaces

When `spark.openivm.managedTablesRoot` is set, initial CREATE data writes use the
logical qualified target identifier plus LOCATION, not a `delta.<path>` CTAS
identifier. This keeps Fabric auto-discovery in the target's real namespace.
Non-managed deployments retain concurrent path CTAS. Ownership, table identity,
precommit cleanup, committed-path reuse and subsequent registration are unchanged.

## Native namespace session isolation

CREATE/REFRESH session clones retain inherited current-catalog/database state
without revalidating unchanged namespaces through public catalog setters. This
preserves Fabric internal database names while still propagating non-default
V2 catalogs and multipart namespaces when their cloned state differs.

## Targeted streaming status polling

`SHOW STREAMING TABLES [IN namespace] [LIKE 'pattern']` returns the existing nine status fields.
LIKE uses Spark SHOW patterns (case-insensitive, `*` wildcard and `|` alternatives) on the unqualified table name, not the backtick-qualified `table_name` result. For a result identity `` `catalog`.`schema`.`table_name` ``, use `IN catalog.schema LIKE 'table_name'`; retain the fully qualified identity when validating the returned row.
Simple literal LIKE names (`[A-Za-z0-9_]+`) resolve the one qualified target\ndirectly without `SHOW TABLES`/namespace enumeration. Wildcard/regex patterns\nand unfiltered SHOW retain catalog enumeration. Selected targets still undergo\nDelta resolution, manifest reads and ownership validation; registered missing\ntargets still report metadata errors.
Poll a specific table with `SHOW STREAMING TABLES IN catalog.schema LIKE 'table_name'`; keep checking identity fields, failures, and `is_active` until the query has actually stopped.
Omitting LIKE retains namespace enumeration and validation. Filtering does not cache ownership metadata or bypass completion waiting.

Spark SQL extension delivering **OpenIVM incremental view maintenance** without Delta CDF.

> Status: under active development

## Runtime support

| Target      | Spark | Delta | Scala   | Java | Maven artifact                |
| ----------- | ----- | ----- | ------- | ---- | ----------------------------- |
| `spark-3.5` | 3.5.1 | 3.2.0 | 2.12.17 | 17   | `ivmextension-spark-3.5_2.12` |
| `spark-4.1` | 4.1.0 | 4.2.0 | 2.13.17 | 21   | `ivmextension-spark-4.1_2.13` |

Spark 3.5 remains the default for every developer command that omits `--target`.

Assembly jars bundle a Linux x86-64 native compiler built and smoke-tested on
Ubuntu 22.04 with its system glibc and libstdc++.

## Documentation

- [Materialized views](docs/MATERIALIZED_VIEWS.md)
- [Standalone streaming tables](docs/STREAMING_TABLES.md)
- [Completed execution telemetry](docs/EXECUTION_TELEMETRY.md)
- [Request-scoped SQL insights](docs/SQL_INSIGHTS.md)
- [Request-scoped SQL log export](docs/QUERY_LOG_EXPORT.md)
- [Activation](docs/ACTIVATION.md)
- [Development](docs/DEVELOPMENT.md)

## License

See [LICENSE](../LICENSE).

### STREAM over catalog views

`STREAM(view_name)` resolves the stored view through Catalyst under its recorded
catalog/namespace, then reads each physical Delta leaf as a stream. Nested and
qualified views preserve output casts, column aliases, projections, branch-local
filters, and `UNION ALL` multiplicity; repeated leaves remain separate source
occurrences in their original order. Relation-local `WITH` reader options apply
to every leaf within that STREAM boundary. Other relations remain batch reads.

Non-Delta/constant leaves, subquery expressions, and reader options that change
the stored input schema (for example enabling CDF on an ordinary Delta view) fail
explicitly. Spark's normal streaming operator/output-mode checks still apply:
wrapping a view does not make arbitrary batch SQL streamable.

Expanded plans, Delta table identities, schemas, and reader options participate
in the normal definition fingerprint. Recreating a base table or changing view
semantics/leaf ordering requires the normal rejection or `onQueryChange=rebuild`
policy. Restarts reuse the managed target checkpoint; no view-specific checkpoint
or offset rewrite is introduced.

### Opt-in public CDF progress without a materialized-view write

`spark.openivm.refresh.publicCdfNoWrite.enabled` (default `false`, Spark startup
configuration) can advance a CDF offset without writing the consumer Delta table.
It is limited to persisted `SIMPLE_PROJECTION` consumers that directly project
attributes from one verified, owned upstream MV. The captured interval must have
complete MERGE history without metadata or protocol changes, or a single
same-table `WRITE` overwrite with literal `true` predicate and complete inferred
CDF (data-changing adds and removes, no explicit CDC files or deletion vectors).
A plain, unpartitioned `WRITE` overwrite without a predicate is admitted only
for versions 0→1 with two small transaction log entries and one small data file
on each side. The initial Delta commit must establish that the exact file
removed was the sole live file; the overwrite must leave only its replacement.
This check reads transaction metadata, not an additional data snapshot. The proof requires an exact signed public row bag, including duplicate and NULL
multiplicity; commit metrics are not a substitute. The replacement path also
supports binary, unpadded strings, `DECIMAL(18,2)`, and microsecond timestamps.
Pins, other replacement intervals, missing identity or history, and unsupported
plans are not eligible. The usual
refresh path remains in charge when the proof is unavailable. This relies on
the existing managed-MV lifecycle locks, not a cross-driver DDL fence.
