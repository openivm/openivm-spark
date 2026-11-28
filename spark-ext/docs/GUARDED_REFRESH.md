# Guarded refresh

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

See [materialized views](MATERIALIZED_VIEWS.md) for ordinary DDL and
[state synchronization](STATE_SYNCHRONIZATION.md) for publication and restore semantics.
