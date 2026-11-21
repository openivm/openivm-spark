# Bounded telemetry queue

`BoundedTelemetryQueue` is an optional best-effort mirror; it does not change or
replace the existing synchronous completed-span exporter or OneLake span sink.
It is disabled by default. Configuration is read once when the owner creates
it from a driver `SparkConf` and Hadoop `Configuration`:

- `spark.openivm.telemetry.queue.enabled`: `true` to enable (default `false`).
- `spark.openivm.telemetry.queue.uri`: legacy single destination, a local path,
  `file:` URI or `abfss:` URI.
- `spark.openivm.telemetry.queue.localUri`: optional local path or `file:` URI.
- `spark.openivm.telemetry.queue.oneLakeUri`: optional `abfss:` URI. OneLake
  requires the host's configured Hadoop ABFS connector and credentials.

When enabled, specify either the legacy URI alone, or one or both explicit
URIs; mixing them is rejected. Setting both explicit URIs mirrors each
accepted record to both destinations.

- `spark.openivm.telemetry.queue.capacity`: maximum queued records (default 1024).
- `spark.openivm.telemetry.queue.maxChars`: maximum UTF-16 characters per record
  (default 262144). Both limits must be positive.

Register `org.openivm.spark.plugin.metrics.OpenIvmMetricsPlugin` in
`spark.plugins`. Its driver plugin constructs the queue once, regardless of
whether Spark-native metrics are enabled, and stops accepting at plugin
shutdown, waiting up to five seconds for the daemon worker to drain. Invalid
configuration disables the queue and logs a generic warning without exposing
the URI; `BoundedTelemetryRuntime.configurationFailureCount` records the failure.
The executor plugin does not create queues.

The shared span and state-sync emission path mirrors records to the queue
without replacing the existing OneLake mirror. It performs no I/O or waiting
for space on the caller thread and writes standalone
JSONL objects. `BoundedTelemetryRuntime.health` exposes the most recent queue's
health, including after shutdown. The lower-level `offer` returns false on
rejection. A shutdown timeout or hard JVM termination can lose records.

Each destination uses a random run prefix and publishes unique objects by
writing a partial object then renaming it. One bounded queue and worker fan out
serially: a slow destination can delay the other, while individual write failures
do not prevent the other write. Queue rejection drops the record for every
configured destination. Global accepted counts records accepted once; global
published counts records published to all destinations, and global export
failures counts records for which any destination failed. Per-destination
accepted, published, dropped, export failures and manifest failures are exposed
in health and fixed-name plugin gauges. Worker failures are counted separately.

After draining, each destination attempts a `*-manifest.json` with UTC times,
aggregate and per-destination counters. Each manifest is a snapshot: its
per-destination `manifest_state` says `not_attempted`, `writing`, `published`,
or `failed` at the time of serialization. Later failures and rejected offers
are reflected in health, not necessarily earlier manifests. Objects may be
absent on filesystem failure, and a failed manifest cannot report its own failure. Consumers must inspect health and the manifest and
must not treat this best-effort mirror as an audit log. Counters have no labels;
span payloads themselves can contain identifiers and require appropriate
access controls. No errors or destination URIs are emitted as metric labels.

When enabled, lifecycle phase records include UTC start time, monotonic start,
inclusive and exclusive nanoseconds, refresh ID, optional request ID and a
parent phase ID. Exclusive time subtracts only directly nested instrumented
phases on the same thread; it does not subtract uninstrumented or asynchronous
work. This is not a complete exclusive-query-time accounting.

Generated SQL and collect boundaries carry an explicit Spark local property.
The driver listener records job-to-stage links, stage attempts and Spark SQL
execution IDs. Stage timestamps come from Spark's wall-clock event times, not
the driver's monotonic clock. Job stage tracking is bounded and marks truncation
in the job-start record. Queue drops can also break links. Checkpoint request
records carry a parent operation ID when a lifecycle trace is active.

When state synchronization is configured, refreshes also report the root
barrier's read-lock wait and hold intervals. They overlap other phases and do
not assert exclusive time. The compiler emits driver and helper phase durations
with the refresh operation ID; the helper uses a separate clock domain and
its events can be lost on forced termination. Malformed or unavailable helper
reports increment `CompileTraceDiagnostics.failureCount` and produce a
rate-limited, sanitized warning.

Checkpoint safety code can inspect the destinations of all live workers,
including timed-out drains, with `BoundedTelemetryRuntime.sinksDisjoint`.
It must use `withSinkTopology` to serialize an epoch check and the guarded
cached-ACK selection with queue registration. The epoch advances before every
registration attempt, even a disabled or unsuccessful one. A disjointness check
on its own is only a snapshot and cannot fence a subsequent registration.
