# Evaluation gates and scoring

[Darwin experiment](../README.md)

## Evaluation flow

1. Trust gate (lock digest) and allowlist gate (changed files vs the lane's lists and the frozen globs).
2. Crash gate: the **crash suite** runs on the baseline (cached, once per box and SHA) and on the candidate.
   If the gate fails, no timed pair runs and no speed is awarded.
3. K >= 3 interleaved pairs of the **perf suite** (baseline-first on even pairs, candidate-first on odd pairs,
   one box lock across all runs). Timed pairs are perf-only; the crash suite is not part of the timing.
4. Scoring against the same-box baseline medians.

The baseline runs in an evaluator-owned git worktree under `<cache-dir>/baseline/<sha12>`; every destructive git
operation (`checkout --force`, `reset --hard`, `clean -fd`) is guarded by `assert_owned_worktree`, which refuses any
path that is not that cache worktree (full SHA, exact parent/name, an owner marker in a real `.git` directory, not
overlapping a protected lane checkout). Fresh baseline and candidate worktrees get `node_modules` through
`npm ci --prefer-offline --no-audit --no-fund` when `node_modules/.bin/nx` is absent.

## Crash gate and recovery qualification

The crash report has one point per `mode/name` where `mode` is `intercept` or `cdf` (reported separately).

- Per point, the candidate is compared with the same-box baseline. The crash gate fails only on a **regression**:
  a flag that fails for the candidate and not for the baseline at the same point, a gated baseline point that
  disappeared, an invalid or unusable report, a timeout, a misused or missing known-gap marker.
- A point the baseline already fails (for example the Delta stale-snapshot replay) stays **visible** in `@FEEDBACK`
  as `baseline fails too`, is never counted as a pass, and never qualifies recovery.
- `recovery_qualified = 1` only when the candidate has zero failing non-known-gap points. `accepted` requires it:
  until then speed is exploratory and cannot be accepted. `crash_fixed_points` counts baseline failures the
  candidate fixes.

**Named witness cases.** `manifest.json` `witness_cases` lists ordinary ScalaTest cases that are gated crash points too:
the fresh-JVM pre/post-DML recoveries on both backends (`InterceptStaleSnapshotCrashSpec`), the stale-snapshot replays
(`InterceptStaleSnapshotDeltaSpec`, `InterceptStaleSnapshotRocksSpec`) and the legacy-intercept bootstrap cases
(`LegacyInterceptWitnessSpec`). The crash suite selector runs them next to `StateGuardCrashRecoverySpec`, one forked JVM
at a time (`state-guard-perf` defaults `OPENIVM_TEST_FORKS` to 1: the crash phases time out when specs fork in parallel), with
ScalaTest's JUnit XML reporter (`-u <report dir>/junit`); the evaluator reads one `TEST-<suite>.xml` per suite, keys each
case by simple suite name plus test name, and appends one `intercept/witness_<name>` point per case. A case that failed,
was skipped or did not run fails its own point (all five flags false), so a candidate that deletes, ignores or breaks a
witness is a regression against a baseline that passes it, and it can never qualify recovery. The suite's own points are
validated first, so a dropped suite point still fails the minimum-gated-points check.

**Known gap.** The point `cdf/mv_version_and_cdf_watermark_gap` (`known_gap=true`) documents the crash between the
MV-version write and the CDF-watermark write. It is documentation only: it is reported separately, never counted as a
pass, never contributes to recovery qualification, and never excuses another point. Stale-snapshot and every other
crash failure stays a visible failure; `known_gap` cannot be used to bypass them.

## Scoring and acceptance

- Invalid (any gate false, allowlist violation, build/test failure, non-finite or missing metrics, timeout,
  trust mismatch) => `combined_score = 0.0`, with `@FEEDBACK` naming the gate and the first failure lines.
- Valid: `combined = max(0.001, 100*clamp(e2e_improvement_median,-1,1) + 50*clamp(wait_reduction,-1,1) + (25 if accepted))`.
  The 0.001 floor keeps a valid-but-slower candidate distinguishable from an invalid 0.0.
- `e2e_improvement_median` = median over pairs of `(baseline_makespan - candidate_makespan) / baseline_makespan`
  (`makespan_ms` == `end_to_end_ms`, includes the final async drain).
- `wait_reduction` = `1 - candidate_targeted / baseline_targeted` (medians over runs of the lane's targeted total).
- **Accepted** only if all gates pass, `recovery_qualified` is 1, `wait_reduction >= 0.80` and
  `e2e_improvement_median >= 0.10`.
- Anti-gaming: `max_concurrent_engine >= baseline median - 1`; `serial_sum_ms` may not fall below 0.5 x baseline while
  makespan rises; `full_refresh_count == 0` and `incremental_count == 6` (an incrementalizable MV must never fall back
  to FULL_REFRESH). Early acknowledgement cannot score: the targeted waits and the makespan include the final drain,
  and parity, metadata and state-sync consistency are checked on the finished run.

Output protocol: stdout carries `@FEEDBACK <one line>` lines then `@RESULT <name>=<finite float>` (clamped to +-1e12).
`@RESULT` names: `combined_score`; gates `allowlist_gate`, `correctness_gate`, `crash_gate`, `incremental_gate`,
`concurrency_gate`, `report_gate`, `trust_gate`; `wait_reduction`; `e2e_improvement_median|min|max`;
`baseline|candidate_targeted_wait_ms`; `baseline|candidate_e2e_ms_median`; `makespan_ms`; `baseline_e2e_spread`;
`accepted`; `recovery_qualified`; `crash_gated_points`, `crash_failing_points`, `crash_baseline_failing_points`,
`crash_fixed_points`; `pairs_completed`; `shared_files_touched`; `changed_file_count`; engine-overlap and
`max_concurrent_engine` medians; lane `info_*_baseline|candidate`; and per MV `mvN_submit_to_ack_ms`,
`mvN_state_barrier_hold_ms`, `mvN_state_barrier_wait_ms`, `mvN_capture_wait_ms`, `mvN_publication_queue_wait_ms` (N = 1..6).
Progress logs go to stderr. The `feedback` artifact is the `@FEEDBACK` lines joined by newlines.

See [lane ownership](LANES.md) and the [report schema](REPORT_SCHEMA.md) for the inputs to these gates.
