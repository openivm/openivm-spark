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

## Deep driver telemetry (opt-in)

Set `spark.openivm.telemetry.deep.enabled=true` to add bounded driver-side
phase, timer and work-counter totals to each completed CREATE/REFRESH span.
The setting is read once when the span starts and gates only that span; a
concurrent or later span with the setting off emits no deep field. With the
setting off (the default), the log line and exported object are byte-identical
to builds without deep telemetry. A reader that predates these fields rejects
deep-on exports, so enable deep telemetry only when every exporter and
consumer in the campaign uses this build.

Deep figures add no SQL, scans, per-row logs, profile rows or per-span
Dropwizard metrics. All `*_ns` values come from `System.nanoTime()` on the driver and
share the emitted `clock_domain_id`; never compare them across clock domains.
Durations measured elsewhere in milliseconds are imported only as
`phase_totals[].external_ms`; they never enter a `*_ns` field.

Top-level fields:

| Field | Meaning |
| --- | --- |
| `clock_domain_id` | Monotonic clock domain of every `*_ns` value. |
| `duration_ns` | Span start to completion. |
| `post_completion_ns` | Completion to emission (durable profile write, export handoff). |
| `owner_thread_wrapped_ns` | Union of owner-thread top-level wrapped phases, clipped to the span. |
| `owner_thread_post_completion_wrapped_ns` | Owner-thread top-level wrapped time after completion. |
| `owner_thread_coverage_status` | `complete`, `incomplete` or `inconsistent`. |
| `owner_thread_coverage_reason` | `active_wrappers_at_freeze` or `wrapped_exceeds_span`, when not complete. |
| `owner_thread_unattributed_ns` | `duration_ns - owner_thread_wrapped_ns`, only when status is `complete`. |
| `owner_thread_overcoverage_ns` | Positive excess, only when status is `inconsistent`. |
| `deep_status` | `augmentation_failed` when deep was requested but rendering failed; no other deep field is then present. |
| `active_wrappers_at_freeze` | Wrappers on any thread still open at emission. |
| `deep_observer_ns` | Recorder self-time; a lower bound, not total overhead. |
| `*_dropped_count`, `deep_names_truncated_count` | Entries refused by the caps, names cut at 128 characters. |

Two label-free driver gauges, registered by the metrics plugin even when
ordinary metrics are off, count deep-requested payload renderings (the log
line and the exported object separately): `openivm.telemetry.deep.augmentation_failures`
counts every rendering whose deep fields could not be added. The `deep_status`
marker is then added by the JSON writer or, if that also fails, by a
parser-free splice, so every JSON-object payload carries it.
`openivm.telemetry.deep.unmarked_payloads` counts only renderings whose input
was not a JSON object and could not be marked, so the record looks deep-off.
A consumer that requested deep telemetry must treat a span with neither
coverage status nor `deep_status` as unavailable, never as deep-off or zero,
and check these gauges.

`phase_totals` (at most 64) has one entry per phase name: `wrapped_count`,
`wrapped_failed_count`, `wrapped_ns`, `wrapped_max_ns`,
`owner_top_level_count`, `off_thread_count`, `external_count`, `external_ms`
and `skipped_count`. Phases are nested and shared, so their totals are not
additive: only owner-thread depth-0 wrappers feed coverage, and nested,
off-thread, imported and skipped observations never do. The unattributed
figure is owner-thread time outside top-level wrappers, not idle or wait time;
`incomplete` and `inconsistent` spans report no unattributed value, never zero.

Phase names are the `RefreshProfile` step names, `profile_flush_persist`,
`perf_<phase>` for each `[openivm-perf]` phase, and these REFRESH internals:
`register_source_delta`, `register_stage_view_ddl`, `register_empty_view`,
`register_projection_probe`, `register_projection_rebind`,
`register_cdf_raw_alias`, `register_cdf_view_ddl`,
`register_source_version_validate`, `rewrite_projection_probe`,
`rewrite_source_snapshot_view`, `cleanup_source_snapshot_view_drop`,
`cleanup_delta_view_uncache`, `cleanup_delta_view_drop`,
`cleanup_fused_scratch_uncache`, `cleanup_fused_scratch_drop`,
`cleanup_window_affected_uncache` and `cleanup_window_affected_drop`.

`timer_totals` (at most 128: `count`, `total_ns`, `max_ns`) mirrors the
`OpenIvmMetrics` timers observed on the owner thread, excluding the
`refresh.phase.*` and `create.phase.*` mirrors of profile steps.
`work_counters` (at most 128: signed `value`) mirrors `OpenIvmMetrics`
counters and adds `refresh.sql_stmt.<kind>.{statements,elapsed_ms,sql_chars,retries}`,
`register.sources`, `register.batch_kind.{staging,source_version,cdf_window,empty}`,
`register.stage_parts`, `register.op_type.<op>` and `cleanup.delta_views`.
Observations on threads without the span (Spark executors, unbound pools) are
not captured; executor work belongs to the task-metrics listener sidecar.

REFRESH completes after cleanup and before the durable profile write, so the
cleanup phases are inside `duration_ns`. CREATE completes when its query span
ends; later publication work appears in `owner_thread_post_completion_wrapped_ns`.

### Interpreting across layers

Keep four intervals separate: the caller's driver command interval, the span
`duration_ns`, Spark job/stage boundaries from the listener sidecar, and the
end-to-end makespan of concurrent requests. SQL return is not durable
publication, and an accepted queue drain snapshot does not prove that every
producer has finished; report a drain or export failure as a gap, not as zero.
Listener stage summaries count only task ends delivered while a tracked job
references the stage. Do not sum them per job, and do not read the listener's
callback self-time as total observer overhead.
