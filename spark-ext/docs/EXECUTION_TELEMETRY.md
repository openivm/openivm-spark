# Completed execution telemetry

Set `spark.openivm.telemetry.uri` to a campaign-scoped Hadoop filesystem URI
to publish one completed JSON object per CREATE/REFRESH request. The request
must also supply nonsecret `openivm.campaign_id`, `openivm.request_id`,
`openivm.correlation_id`, `openivm.node_id`, and `openivm.phase` local
properties; the campaign, correlation, and phase values may instead use the
`spark.openivm.telemetry.campaignId`,
`spark.openivm.telemetry.correlationId`, and
`spark.openivm.telemetry.phase` configuration keys.

Version 1 objects are atomically renamed from `_temporary/v1/*.partial` to
`completed/v1/<sha256-execution-identity>.json`. Consumers ingest only the
completed path. Re-publishing identical content is idempotent; different
content for the same identity fails. The public constants live in
`OpenIvmTelemetryContract`, and the packaged schema is
`openivm-telemetry-span-v1.schema.json`. Export failures fail the SQL operation;
when the URI is unset, the existing log-only span behavior is unchanged.
Same-request retries of `CREATE MATERIALIZED VIEW IF NOT EXISTS` and
already-applied `ADVANCE SOURCE VERSIONS` reuse only a complete, identity-matched
accepted object; a new request identity publishes a complete explicit
short-circuit outcome. The schema's
`x-openivm-w6-required-success-fields` annotation is the ingestion-required
field set for every accepted outcome. `create_already_exists` is valid only
when `operation=create`; the schema's operation/outcome map is authoritative.
Reusable successes require a nonempty `source_versions` array that is unique
and ascending by lower-cased canonical relation key. The internal duration may
differ from the timestamp interval by at most 5 ms.

When present, `lock_acquisitions` reports each canonical MV lifecycle key held
by the command: `mv_key`, `lock_kind` (`mv_data` or `create_publication`),
`mode` (`read` or `write`), `requested_epoch_ms`, `acquired_epoch_ms`,
`released_epoch_ms`, `wait_ms`, `hold_ms`, `thread`, and the available
`dbt_node_id` and `request_id`. New entries always emit `lock_kind`; it remains
optional in the v1 JSON schema for older payloads. Compare lock occupancy only
within the same (`lock_kind`, `mv_key`) domain; concurrent upstream readers are
specifically `mv_data` entries with mode `read`.

Refresh writes its target and reads each authoritative direct upstream;
lifecycle mutations retain write locks. CREATE additionally holds a
`create_publication` write guard. Its exported span retains both lock kinds
with final release timestamps, and is emitted only after both guards release.
The legacy `same_mv_lock_wait_ms` equals the sum of all recorded `wait_ms`,
including CREATE publication waits; `driver_admission_wait_ms` continues to
report admission separately.

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

## Optional best-effort mirror

The [bounded telemetry queue](BOUNDED_TELEMETRY_QUEUE.md) can mirror records to local and OneLake destinations without replacing the synchronous export contract above.
