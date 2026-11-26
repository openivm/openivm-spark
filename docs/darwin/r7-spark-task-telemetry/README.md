# Round 7 slot 3: Spark task-execution telemetry ledger

Status: **measurement infrastructure, review-only.** The R7 release scope was frozen as v2 + D5 at the 19:11Z no-new-hypotheses cutoff. This lane is not in it. This ledger records what was built, qualified and measured, and what was cancelled by the freeze. It is not a product candidate, an optimization score or Darwin acceptance evidence.

## Identity

| Item               | Value                                                                                                                                                                                                                                                                                                                            |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Lane branch        | `darwin-r7-s3-task-telemetry` (never pushed to #108 or `main`)                                                                                                                                                                                                                                                                   |
| Code range         | `50fa4ceb2bfbe2419eb682503cfe20be2083f388..7791d7953bbb77fbbcfb74cba18edd7cc8e02de3`                                                                                                                                                                                                                                             |
| Production commit  | `8541fc98484befd1d7c7050cba80e83c58f3545e` (direct child of deployed v2 `50fa4ceb`)                                                                                                                                                                                                                                              |
| Head of code range | `7791d7953bbb77fbbcfb74cba18edd7cc8e02de3`, tree `55b5c1d89a6a107540c71a1a13ff9b799a56560a`. Test-only review discriminators.                                                                                                                                                                                                    |
| This ledger        | Docs-only commit on top of `7791d795`. It changes no code, test, build, native pin, evaluator or span-v1 schema.                                                                                                                                                                                                                 |
| Devbox             | Privy lease `ls-78c398a348ec4e67b4208dc561895908`, group `dev/raki-windows-15`, devcontainer member only. One lease; no other timed workload on the box.                                                                                                                                                                         |
| Image              | Content tag `09a5d1572aca7d969f8b087bee443f3c254fa86943e597758aa8f1b2d9591c2e`, base `sha256:700dd89780736c1f3d9f9c72de5ef340437bb1a9c3e221dffd40588adfd286ad`. That base's layer list is a prefix of the running UID-adjusted image `sha256:ebd5d89aadb95e4a791ec8f4ee8369ca5bb8f74489c7d7a4761961616e3a3e48`.                  |
| Native             | OpenIVM `57b21876b866b2fa7176a3ad35246f4daf90287e`, LPTS `84390b1246d229ee584db74dd567a181db071227`. Extension `55aa106dcc3904831a74edba91531e1cb579e62aceeee6c7afe63c88ceb1b05e`; CLI `c41f957db5645d545a9caef430994cf262d24fad9297fef70c691395aadb43fb` (`v2.0.0 (Cyanoptera) d54cbbe3df`). Both match the image `SHA256SUMS`. |
| Runtimes           | Spark 3.5.1 / Delta 3.2.0 / JDK 17.0.20.1. Spark 4.1.0 / Delta 4.2.0 / JDK 21.0.12.1. 16 visible CPUs, 61 GiB.                                                                                                                                                                                                                   |

D8 reviewed exact `7791d795` (both 17/17 logs, the probe and the 18 observer arms) and gave no code-level veto, subject to the documented tracked-owner and wrapped-body limits. D1 accepted `50fa4ceb..7791d795` as the slot-3 input to base-r2, with `queue`, `deep` and `taskMetrics` pinned identically in both product arms. That acceptance is not common-baseline or final-artifact clearance.

## What changed

The existing driver plugin installs `TelemetryJobListener` only when the bounded queue exists. Setting `spark.openivm.telemetry.taskMetrics.enabled=true` adds physical task summaries. Both flags default to `false`. Collection adds:

- no executor-side plugin code;
- no scheduled job or scan;
- no per-row or per-task export.

It uses `SparkListenerTaskEnd` on the existing listener bus. It enriches the existing single `spark_stage_completed` record per stage attempt with an optional `task_summary`. The span-v1 schema, `phase` records and `effective_refresh_type` records are unchanged. The driver/schema lane (`darwin-r7-driver-telemetry`) owns the consumer documentation and the exporter validator.

## Coverage and attribution

| Surface                        | Meaning / limit                                                                                                                                                                                                                                                                                                                                                                                          |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| MV/request to job              | Optional **inherited** `request_id`. `operation_id` comes from `openivm.telemetry.refresh_id` and matches `PhaseTimeline.refresh_id`. Also carries the generated action/boundary property and the SQL execution ID when Spark supplies one. Request IDs are never synthesized; rejected identity strings are counted, not emitted.                                                                       |
| Job to stage                   | Job-start `stage_ids` are references, not exclusive ownership. A job that reuses a completed stage references old physical work without creating new work. Join on the driver `clock_domain_id`. Never aggregate a stage summary once per referencing job.                                                                                                                                               |
| Stage attempt to task attempts | A summary starts only on an observed submission of a tracked stage. Task ends must match stage ID and attempt. Completion consumes the summary. A missing submission or capacity loss yields `submission_not_tracked`, not zeros. An observation exception invalidates the whole summary (`callback_error`).                                                                                             |
| Physical work vs logical rows  | Counters cover observed physical task ends, including failed, killed, retried and speculative work. `Resubmitted` is a notification, not another completed attempt. `nonfirst_attempts` is an attempt-number observation, not an inferred retry.                                                                                                                                                         |
| Executor metrics               | Run, deserialize, GC and result-serialization time (ms); CPU and deserialize CPU (ns); input/output and shuffle records and bytes; remote shuffle bytes and fetch wait; shuffle-write ns; memory and disk spill. `attempts_with_metrics` / `attempts_without_metrics` describe the actual carriers. A partial sum has a `partial_metric_counts` entry. A metric with zero carriers is absent, never `0`. |
| Durations / skew / scheduling  | Count, sum and max of driver-observed task intervals; the minimum observed launch; the stage-submit-to-first-launch gap. Max vs mean indicates skew; it is not a percentile. None of these are exclusive scheduling delays or commit times. No driver-minus-executor clock subtraction.                                                                                                                  |
| Bounded drill-down             | The first eight task identities per stage attempt, ordered by observed completion. **Not** representative. `sample_truncated` and `sample_dropped_count` are explicit; the cap does not limit numeric aggregation.                                                                                                                                                                                       |
| Bounds and health              | 1,024 jobs, 256 unique stages per job, 8,192 active references, 512 active summaries. Label-free `openivm.telemetry.listener.*` gauges count truncation, capacity drops, unmatched tracked task ends, abandoned attempts, callback errors, rejected identities, wrapped callback-body count/time and application-wide executor removals.                                                                 |

### Residual correlation limits

- **Tracked-owner window.** After the last referencing job ends, a late task end (for example a killed task of an abandoned stage) is indistinguishable from unrelated work. It is neither accumulated nor counted as unmatched. Completeness covers the tracked-owner window only, not all physical attempt work.
- **Wrapped-body timer.** `observed_callback_ns` measures the wrapped callback body only. It excludes identity checks, prefilters and the executor-removal callback. It is not total listener cost.
- **Unrecoverable work.** Lost Spark bus events, callers that never set an identity, and Delta/internal jobs without a request property cannot be attributed. See the census below.
- **Unobserved is not zero.** Off/legacy arms never observe task metrics; their zeros are sentinels for "unobserved".

## Focused qualification

```bash
npx --no-install nx run spark-ext:fmt
npx --no-install nx run spark-ext:test --configuration=spark-3.5 -- 'ivmCommon/testOnly org.openivm.spark.telemetry.TelemetryJobListenerSpec org.openivm.spark.telemetry.BoundedTelemetryQueueSpec'
npx --no-install nx run spark-ext:test --configuration=spark-4.1 -- 'ivmCommon/testOnly org.openivm.spark.telemetry.TelemetryJobListenerSpec org.openivm.spark.telemetry.BoundedTelemetryQueueSpec'
```

Each runtime ran 2 suites: 17/17 passed, 0 failed/ignored/pending. Coverage:

- a real Spark shuffle with job/action/request/operation linkage;
- shared references after an owner ends, and reuse without new execution;
- speculation, retry and executor loss;
- null and mixed metric carriers;
- caps and late stages, and a summary invalidated on overflow;
- emitter exceptions;
- deep-off behavior;
- a killed task end after the last referencing job, which emits no summary and does not increment unmatched.

These are source-level targeted tests, not an assembled-JAR, Fabric or PR-tip CI qualification.

## Full regression: `spark-ext:verify-all` on exact `7791d795`

Admission at 2026-11-26T13:37:35Z through Privy job `7e98395bbc2a` on `dev/raki-windows-15_devcontainer`. The guards were `HEAD == 7791d795`, a clean worktree and `OPENIVM_TEST_FORKS=8`. The command was:

```bash
npx --no-install nx run spark-ext:verify-all > /tmp/r7-s3-verify-all-7791d795.log 2>&1
```

The hub later returned 404 for that job ID. The guarded bash (PID 13835) and `nx` (PID 13845) remained traceable by PID and log. The session event log retains the admission (`313c6445-6dd3-411f-a8be-fd5b95576076`, 13:37:34.552Z) and the read showing the job ID (`7ea60ff0-e2fc-4993-82b5-cdef57ca18b4`, 13:53:40.453Z). The inventory count `489` is suites (inventory entries), not cases. Each runtime phase is reported separately.

### Spark 3.5 phase (complete, green)

| Module        | Run                                                          | Suites                   | Tests                                                         |
| ------------- | ------------------------------------------------------------ | ------------------------ | ------------------------------------------------------------- |
| ivm-compiler  | "Run completed in 2 hours, 45 minutes, 6 seconds." 488 run   | completed 13, aborted 0  | succeeded 488, failed 0, canceled 0, ignored 0, pending 0     |
| ivm-common    | "Run completed in 2 hours, 45 minutes, 6 seconds." 642 run   | completed 57, aborted 0  | succeeded 642, failed 0, canceled 2, ignored 0, pending 0     |
| ivm-extension | "Run completed in 2 hours, 45 minutes, 6 seconds." 492 run   | completed 55, aborted 0  | succeeded 492, failed 0, canceled 0, ignored 0, pending 0     |
| ivm-it        | "Run completed in 2 hours, 47 minutes, 30 seconds." 1909 run | completed 364, aborted 0 | succeeded 1909, failed 0, canceled 0, ignored 121, pending 14 |

Every module printed `All tests passed.` The phase ended with `[success] Total time: 10052 s (02:47:32), completed Nov 26, 2026, 4:27:44 PM`.

### Spark 4.1 phase

The 4.1 phase is complete and green. It wrote its inventory as `Wrote 489 tests to …/spark-4.1.txt`.

| Module        | Run                                                         | Suites                   | Tests                                                         |
| ------------- | ----------------------------------------------------------- | ------------------------ | ------------------------------------------------------------- |
| ivm-common    | "Run completed in 2 hours, 55 minutes, 49 seconds." 642 run | completed 57, aborted 0  | succeeded 642, failed 0, canceled 2, ignored 0, pending 0     |
| ivm-compiler  | "Run completed in 2 hours, 55 minutes, 50 seconds." 488 run | completed 13, aborted 0  | succeeded 488, failed 0, canceled 0, ignored 0, pending 0     |
| ivm-extension | "Run completed in 2 hours, 55 minutes, 50 seconds." 492 run | completed 55, aborted 0  | succeeded 492, failed 0, canceled 0, ignored 0, pending 0     |
| ivm-it        | "Run completed in 3 hours, 1 minute, 11 seconds." 1909 run  | completed 364, aborted 0 | succeeded 1909, failed 0, canceled 0, ignored 121, pending 14 |

Every module printed `All tests passed.` The phase ended with `[success] Total time: 10873 s (03:01:13), completed Nov 26, 2026, 7:31:35 PM`.

### Terminal status

The run is **terminal and green**:

- The inventory check printed `[compare-test-inventory] identical Spark 3.5 and Spark 4.1 test inventories`.
- Nx printed `NX   Successfully ran target verify-all for project spark-ext`, with `Run duration: 354m 0s` and `Cache: 0/1 hit (0%)`.
- The log was last written at 2026-11-26T19:31:36Z. The guarded bash (PID 13835) exited, and `nx` (PID 13845) is reaped-pending (`Z`) under the container's PID 1.

The process exit code is **unavailable** because the hub no longer serves job `7e98395bbc2a`. The Nx success line above is the terminal-status evidence.

| Log                                                        |     Bytes | SHA-256                                                            |
| ---------------------------------------------------------- | --------: | ------------------------------------------------------------------ |
| Raw `/tmp/r7-s3-verify-all-7791d795.log`                   | 2,094,959 | `57a633d58d0d66db49b4f98d0605fa9c035d4ab2e5f498f6de4ecaeeefc62cc2` |
| ANSI-stripped, gzipped copy (session store, not committed) |   368,417 | `c0b5e4d996eb77f1ef0cd1894f24ef1479da9b72822a5d73f3f0c827f42d5c88` |

In the log, `ERROR` lines mentioning `COMPILE_FAILED` / `FULL_REFRESH` and `state_lag_full_refresh` warnings are expected test output from negative and fallback cases, not failures.

## CDF observer probe: functional dry runs only

`receipts/CdfObserverProbe.java` is a local replica of D7's six-MV CDF CREATE/REFRESH protocol. It uses one fresh JVM per arm: `off` (queue disabled), `legacy` (queue on, taskMetrics off) or `deep` (queue on, taskMetrics on). Settings:

- `spark.openivm.changeFeed.mode=cdf`, Delta catalog backend, synchronous state sync;
- profile, query log and bounded queue live; 4 shuffle partitions; local JSONL export.

Each cycle creates two CDF sources. It then runs four six-MV batches: `create_serial_source` (6 workers), `refresh_serial` (1 worker), `create_parallel_source` (6 workers) and `refresh_parallel` (6 workers). Each command:

1. goes through `FabricCommandBridge` with a unique request/job group;
2. pages `SHOW OPENIVM INSIGHTS`;
3. calls `awaitDrain(10000)`;
4. releases its query-log captures, retrying the release race.

Bidirectional `EXCEPT ALL` parity and the requirement that every REFRESH stays incremental run outside timing. One warmup cycle precedes the measured cycle.

**These dry runs ran niced while verify-all occupied the box. Their timings are functional only and must never be cited as cost.**

| Fact                                                 |                                 Spark 3.5.1 / JDK 17 |       Spark 4.1.0 / JDK 21 |
| ---------------------------------------------------- | ---------------------------------------------------: | -------------------------: |
| Measured commands / effective type                   |                           24 / all `AGGREGATE_GROUP` | 24 / all `AGGREGATE_GROUP` |
| Parity / incremental assertions                      |                                               passed |                     passed |
| Per-command `awaitDrain(10s)` timeouts               | 6 (all in `refresh_parallel`, under verify-all load) |                          0 |
| Final drain / queue drops / export failures          |                                         true / 0 / 0 |               true / 0 / 0 |
| Query-log release retries (all cycles)               |                                                   78 |                        153 |
| JSONL records / bytes (all cycles)                   |                                    7,257 / 4,889,139 |          6,978 / 4,669,706 |
| Stage completions / with `task_summary` / duplicates |                                    1,365 / 1,365 / 0 |          1,256 / 1,256 / 0 |
| Job-start stage truncation                           |                                                    0 |                          0 |
| Measured REFRESH job starts / with `operation_id`    |                                            472 / 433 |                  461 / 426 |
| Task attempts summarized / without metrics           |                                           15,375 / 0 |                 15,173 / 0 |
| Sampled identities dropped by the 8-per-attempt cap  |                                               11,634 |                     11,718 |
| Failed / killed / speculative / non-first attempts   |                                        0 / 0 / 0 / 0 |              0 / 0 / 0 / 0 |

Job census in the measured cycle, from a census listener present in every arm:

- Spark 4.1: 1,297 jobs, 688 with an `openivm.request_id`, 23,252 tasks. All 609 jobs without a request ID fall into 19 description buckets: 394 `<none>`, 126 `Delta: Filtering files for query`, 87 Delta snapshot jobs and 2 `recordDeltaOperationInternal`.
- Spark 3.5: 1,336 jobs, 714 with a request ID, 23,002 tasks.

The census window spans the whole measured cycle, including source DDL/DML and the parity checks outside timing. These unattributed jobs therefore cannot be charged to MV commands, and the listener does not try to.

Receipt caveat: the 3.5 inventory (`task_summary_integral_sums`) predates an analyzer fix. It also sums per-stage `duration_max_ms` and `first_task_launch_epoch_ms`, so those two 3.5 values are meaningless. The 4.1 inventory (`task_summary_aggregates_all_cycles_sum_or_max`) takes the max of maxima and omits epochs.

### Timed CDF arms: cancelled, never started

An 18-arm timed design (`receipts/cdf-arms.sh`; Latin-square-first ordering of `off`/`legacy`/`deep` per runtime, analyzed by `receipts/cdf_arms_analyze.py`) was armed to start after verify-all. The coordinator freeze ("Do not start any new run") arrived first. The watcher was disarmed at 2026-11-26T19:16:46Z, logging `reason=coordinator_freeze_no_new_runs`, before any arm started. **No CDF observer-overhead measurement exists from this lane.**

## Observer cost evidence that does exist

All of it is diagnostic. None of it is a ranking, equivalence or zero-overhead claim.

1. **Generic local Spark SQL, 18 arms** (`receipts/SparkTaskObserverProbe.java`, `receipts/generic-observer-arms.json`):
   - Setup: Spark 4.1, local[2], six permutations × three arms, six collect-and-validate aggregations per JVM after four warmups.
   - Deep vs legacy paired drained-time differences spanned **−22.1% to +18.1%**; the sign is unresolved.
   - Deep vs queue-off paired median was **+9.4% drained**. That includes the pre-existing queue, async listener and durable local export, so it is **not** marginal task-aggregation cost.
   - Deep wrote **~4.13×** the JSONL bytes of legacy (44,823–44,863 vs 10,864) for the same 24 records.
2. **Callback replay** (`receipts/TaskSummaryReplay.java`, `receipts/callback-replay-raw.txt`):
   - Measures callback + JSON only, with no Spark dispatch, queue or filesystem.
   - Deep vs legacy added 27.428 µs per 32-task stage (≈857 ns/task) and 119.519 µs per 1,024-task stage (≈117 ns/task).
   - This is synthetic and kept outside end-to-end overhead.
3. **v2 listener baseline replay** (`receipts/TaskObserverReplay.java`): 3.295 µs per callback group for untouched v2 vs a no-op listener. It is not comparable with item 2.
4. **D1/D8 locked diagnostics** (not produced by this lane):
   - Deep-on vs off on one binary: +2.95% median durable cost over three pairs.
   - Default-off combined vs untouched v2: mixed signs (−2.38% median durable, +1.61% return).

## Receipts

Files in `receipts/` (SHA-256):

| File                          | SHA-256                                                            |
| ----------------------------- | ------------------------------------------------------------------ |
| `CdfObserverProbe.java`       | `32762a300d3b23eda2e7414777e385e46b894607eac759d56f25e062ec66e26d` |
| `cdf_arms_analyze.py`         | `d4344bf338f45e7d31f1c9028f0e7cc9e7e06e12c58a471ad617abefeea86d04` |
| `cdf-run.sh`                  | `6f1b84603c168c570ad4bec6a1e99886372d0a29c5361e48f581835a092b1c91` |
| `cdf-arms.sh`                 | `422ef1ebcdd1566bf0420eb27b5a4759d2a0e693a19345fceed54c207934a025` |
| `cdf-dry-spark-3.5-deep.json` | `eb01fab8b7ddcebaac345e9b3c2a4517c9ce1ef7881a8fe3077ea1b47bfe1bd5` |
| `cdf-dry-spark-4.1-deep.json` | `f2c3f0f821511936cf9dd8a9706bff02b67c0ed9e5459f6d2e569a2e64b1a33f` |
| `SparkTaskObserverProbe.java` | `d62a6158fe5d22d28401c38ed4d5f05d4e013e62b3f361c9966ec548d6713cc5` |
| `generic-observer-arms.json`  | `0fd8de5cd9f404b2e96f6c425a95a4b8b6181f495dafb24295895b587472e26b` |
| `TaskSummaryReplay.java`      | `8aea11567d0b5d74465791462196fec1a6dcdedb487299ed77b21d3047d9154f` |
| `callback-replay-raw.txt`     | `1142413c16b8c98d1c80de56d0cf768b97e933a389e521e1a7e5f2382d3c3b4a` |
| `TaskObserverReplay.java`     | `8d20e950709ba5de8b67475208350bb34a0d1a978cb59e45152d3601efcd91f1` |

Not committed: the dry-run logs (`dry35-deep.log` `3975d669…449e`, `dry41-deep.log` `c523fc16…e014`) and the classpath exports (`cp35.export` `3914d74e…298`, `cp41.export` `9527074a…e234`). They are host-specific and stay in the session store.
