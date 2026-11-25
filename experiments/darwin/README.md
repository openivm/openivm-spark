# Darwin state-guard experiment

Trusted evaluator and lane configs for evolving OpenIVM-Spark product code (Scala) with Darwin so
that concurrent materialized-view (MV) refresh spends less time contending on the state guard.
Five independent lanes (A-E) each get their own `darwin evolve` run. A candidate is only credited
when the crash-recovery and correctness gates pass, every MV stays incremental, and it is measurably
faster than a fresh baseline measured on the same box.

## Evidence class (read first)

The suite measures **local modeled latency** (`evidence_class: local-relative-modeled-latency`).
It is relative evidence for ranking candidates on one box. It is **not** OneLake proof, and it does
not prove that Delta I/O is the cause of the contention. The measured spans are nested and
non-additive (catalog finalization runs inside the barrier hold, waits overlap), so a sum of spans
is not a critical path. Reference numbers from the earlier modeled run, kept in `manifest.json` as
`reference_evidence` for documentation and feedback only (never scored):

| Observation | Seconds |
| --- | --- |
| MV6 capture wait overlapping MV5's root read-lock hold | 5.509 |
| MV5 catalog-finalization span inside that lock | 12.439 |
| MV1 publication-queue wait | 5.817 |
| MV6 publication | 6.119 |
| Six engine executions overlapped | 31.809 |

## Layout

```
experiments/darwin/
  manifest.json            single machine-read source of truth (allowlists, frozen globs, acceptance, budgets, lane targets)
  manifest.yaml            human mirror; NOT parsed by any code
  evaluator.lock.json      sha256 of evaluator/*.py + manifest.json (the trust anchor)
  evaluator/               state_guard_eval.py (CLI), policy, report, scoring, runner, lock
  configs/lane-{a..e}.yaml `darwin evolve` experiment files, one per lane
  fixtures/                synthetic report sets + diff cases; make_fixtures.py regenerates them (--check verifies)
  tests/                   unittest suite (offline: no Darwin, no Nx, no Spark)
  provision-agent-tools.sh pinned Copilot CLI (+ optional Darwin wheel) installer with sha256 checks
  validate-offline.sh      runs the offline suite from the repo root
```

## Prerequisites

The evaluator itself is stdlib-only Python 3.12 plus `bash`. Java, Scala, sbt, Spark and Delta only
ever run in the pinned devcontainer through Nx (`npx --no-install nx run spark-ext:state-guard-perf`).

**Darwin and the Copilot CLI** (verified on Ubuntu 24.04):

| Component | Pin |
| --- | --- |
| Darwin | `darwin_evolution` 0.3.0 wheel (`darwin --version` prints `0.3.0`), sha256 `e5432cc6457dc6c65272d872082f71cbbd439d869db2d591b773e92cbc915f0e`, source commit `c50a954b7db43522f1f4012572e52e48c97a561c`, built with the project's setuptools backend from that tree |
| Copilot CLI | **1.0.67** (`github-copilot-sdk` 1.0.5, which Darwin 0.3.0 drives, requires it; CLI 1.0.5 fails with `Unhandled method connect/runtime.shutdown`). Tarball `copilot-linux-x64.tar.gz` of release `v1.0.67` sha256 `c6d251de20d14415ebd6af00bfb6a8e3854094b49fa49fb7f3df2bfa64f6db3b`, extracted binary sha256 `a78b6e40e447fbadf500ac04e05e7f1378e73ffe96c67d3fff46f27200e34bc1` |

The PyInstaller-built Darwin binary does not run on Ubuntu 24.04; use the wheel.

```bash
DARWIN_WHEEL=/path/to/darwin_evolution-0.3.0-py3-none-any.whl bash experiments/darwin/provision-agent-tools.sh
```

The script is idempotent, verifies both checksums, installs the CLI under `~/.darwin-tools/copilot-cli/1.0.67/bin/copilot`
(override with `DARWIN_TOOLS_DIR`), installs Darwin into `~/.darwin-tools/venv`, and prints the `PATH` and
`COPILOT_CLI_PATH` exports to use. Install the wheel as `python3 -m venv --without-pip V && python3 -m pip --python V/bin/python install pip && V/bin/python -m pip install <wheel>`
if you prefer to do it by hand.

**Identity.** Darwin reads `COPILOT_GITHUB_TOKEN`, `COPILOT_GH_HOST` and `COPILOT_GH_ACCOUNT` (and
`COPILOT_CLI_PATH`) from an env file. On a devbox that file is the repo's gitignored `.env`, delivered by
repo-bootstrapper's `sync/<repo>` folder; never put these values in job code, configs or logs. The
proven setup is GitHub Enterprise: `COPILOT_GH_HOST=msft.ghe.com`, `COPILOT_GH_ACCOUNT=mdrrahman@msft.ghe.com`.
The agent model in every lane config is `claude-haiku-4.5`, the model proven end to end in the preflight;
use a local copy of a lane config to try another model. The evaluator strips `*TOKEN*`, `*SECRET*`,
`*PASSWORD*`, `*CREDENTIAL*` and `*API_KEY*` variables before running the suite and redacts token-looking
text from everything it prints.

## Trusted evaluator checkout

Candidates edit a workspace copy of the repo, so the evaluator must run from a copy they cannot touch.
Make one pinned checkout of the **frozen harness commit** outside the lane checkouts, and use that SHA
as the baseline too:

```bash
export OPENIVM_DARWIN_BASELINE_SHA=<frozen harness commit, full sha>
git worktree add /tmp/darwin-trusted "$OPENIVM_DARWIN_BASELINE_SHA"
export OPENIVM_DARWIN_TRUSTED=/tmp/darwin-trusted
export OPENIVM_DARWIN_LOCK_SHA256=$(python3 "$OPENIVM_DARWIN_TRUSTED/experiments/darwin/evaluator/lock.py" --digest)
```

The lane configs call `$OPENIVM_DARWIN_TRUSTED/experiments/darwin/evaluator/state_guard_eval.py`, which
recomputes the digest of its own `evaluator/*.py` + `manifest.json` against `evaluator.lock.json` and the
pinned `OPENIVM_DARWIN_LOCK_SHA256`; a mismatch scores 0.0 with `trust_gate=0`. Any candidate diff under
`experiments/**` is also a frozen-file violation. After ANY edit to `evaluator/*.py` or `manifest.json`, run
`python3 experiments/darwin/evaluator/lock.py --write` and re-pin the digest.

The workspace Darwin hands the evaluator is a git repo with the candidate as HEAD and **no remotes**, so
the configs pass `--baseline-repo "$OPENIVM_DARWIN_TRUSTED"` and the evaluator fetches the baseline SHA from there.

## Running a lane

One lane per branch checkout (Darwin evolves committed HEAD; candidates stay in local refs until promoted):

```bash
cd <lane checkout>        # contains the frozen harness commit
darwin evolve experiments/darwin/configs/lane-a.yaml \
  --home /tmp/darwin-home-a --env-file /workspaces/openivm-spark/.env --repo .
```

Likewise `lane-b` .. `lane-e`. Each lane: `max_iterations: 18`, early stop after 8 iterations without
improvement of `combined_score` (absolute `min_improvement: 0.5`; the score spans roughly 0-175), agent
timeout 3600 s, evaluator timeout 7200 s per iteration. The agent only edits files; the evaluator owns builds,
tests and benchmarks.

Evaluator commands (inside each config) shape:

```bash
python3 "$OPENIVM_DARWIN_TRUSTED/experiments/darwin/evaluator/state_guard_eval.py" --mode candidate --lane a \
  --workspace "$PWD" --baseline-repo "$OPENIVM_DARWIN_TRUSTED" --baseline-ref "$OPENIVM_DARWIN_BASELINE_SHA" \
  --lock-sha256 "$OPENIVM_DARWIN_LOCK_SHA256" --pairs 3
```

`--workspace "$PWD"` is deliberate: Darwin runs the command inside the candidate checkout, and the evaluator resolves the
path to an absolute one before anything derives from it (a relative path used to put the test JVM's crash report
under `spark-ext/` where the evaluator never looked, scoring every candidate 0). Darwin 0.3.0 also discards the
evaluator's stdout for the *initial* (seed) program, so the evaluator keeps its last result block in
`~/.cache/openivm-darwin/last-evaluation/<lane>-<mode>.txt`.

Metrics are strict lookups of `@RESULT` lines: a missing metric makes Darwin treat the evaluation as failed
instead of silently scoring 0. **Darwin 0.3.0 counts failed evaluations toward `max_iterations_without_improvement`
and the evaluator cannot prevent that.** An evaluator infrastructure error (exit 2, no `@RESULT`, `infra_error` in
the log) is therefore distinguishable but not excluded from patience or budget; re-run or resume a lane whose
iterations were lost to infrastructure.

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

## Lanes and ownership

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

## Manual scoring

```bash
python3 experiments/darwin/evaluator/state_guard_eval.py --mode candidate \
  --workspace <candidate-checkout> --baseline-ref <sha> --lane a --pairs 3 --spark-target spark-3.5 \
  --lock-sha256 "$(python3 experiments/darwin/evaluator/lock.py --digest)"
```

Baseline sanity check (crash suite once, then K perf runs; always prints `combined_score=0.000000`):
`--mode baseline --workspace <checkout> --lane a`. Offline: add `--offline-fixtures experiments/darwin/fixtures/reports
--offline-candidate-set good-candidate --skip-lock-check`. Exit code 0 for every candidate outcome (invalid scores 0.0);
exit 2 only for evaluator infrastructure errors, still with `combined_score=0.000000`. `--spark-target spark-4.1` selects
the Spark 4.1 / Delta 4.2 target; both targets use the same discovery and the same suites.

The suites and their report contracts (`state-guard-perf.json`, `state-guard-crash.json`, `state-guard-events.jsonl`) are
written to `<root>/.logs/state-guard/<label>/` (`OPENIVM_STATE_GUARD_REPORT_DIR`). `OPENIVM_STATE_GUARD_SUITE`
(`all|perf|crash`) selects which suite the Nx target runs; `OPENIVM_STATE_GUARD_CATALOG_BACKEND` (`delta|rocksdb`) selects the
catalog backend of the perf suite.

## Offline validation

No Darwin, Nx, Spark or network needed:

```bash
bash experiments/darwin/validate-offline.sh                    # = python3 -m unittest discover -s experiments/darwin/tests -t .
python3 experiments/darwin/fixtures/make_fixtures.py --check   # fixtures match the generator
python3 experiments/darwin/evaluator/lock.py --verify          # lock matches the evaluator files
```

Tests write scratch files to `experiments/darwin/.scratch/` (git-ignore it or delete it) and print only test status.
They also load every lane config with PyYAML and assert the `darwin evolve` schema, the command pieces, the metric list
(equal to what the evaluator emits) and the absence of credentials.

## Report schema

`state-guard-perf.json` (`openivm.state-guard-perf/v1`), all numbers finite and `>= 0`; an unknown `evidence_class` is invalid:

```
{ "schema": "openivm.state-guard-perf/v1", "evidence_class": "local-relative-modeled-latency",
  "spark_target": "spark-3.5|spark-4.1", "head_sha": str,
  "latency_profile": {"name", "catalog_read_ms", "catalog_write_ms", "state_read_ms", "state_write_ms", "state_rename_ms", "scale"},
  "end_to_end_ms": num,            // must equal makespan_ms
  "makespan_ms": num, "drain_ms": num, "serial_sum_ms": num, "max_concurrent_engine": int,
  "engine_overlap_ms": num, "barrier_hold_overlap_ms": num,
  "views": [ // exactly 6, mv_index 1..6
    {"mv_index", "name", "wall_ms", "refresh_type", "outcome", "submit_to_ack_ms",
     "state_barrier_wait_ms", "state_barrier_hold_ms", "capture_wait_ms", "catalog_finalize_ms",
     "publication_queue_wait_ms", "publication_ms", "engine_exec_ms",
     "submit_at_ms", "ack_at_ms", "barrier_acquired_at_ms", "barrier_released_at_ms",
     "engine_start_at_ms", "engine_end_at_ms", "catalog_finalize_start_at_ms", "catalog_finalize_end_at_ms",
     "publication_start_at_ms", "publication_end_at_ms"} ],   // *_at_ms: offsets from batch start
  "counts": {"incremental_count", "full_refresh_count",
     "fs_ops": {"state_sync": {"read", "write", "rename", "delete", "list"}, "catalog": {same}},
     "state_sync": {"publications", "commits", "snapshots", "bytes_uploaded"}},
  "gates": {"parity": bool, "incremental_only": bool, "state_sync_consistent": bool},
  "notes": [str] }
```

`state-guard-crash.json` (`openivm.state-guard-crash/v1`):
`{"points": [{"name", "mode", "crashed", "recovered", "parity", "metadata_converged", "incremental_after_recovery", "known_gap", "notes"}]}`.
`evaluator/report.py` is the validator and the exact contract. Beyond field types it checks that
`end_to_end_ms == makespan_ms`, `serial_sum_ms == sum(submit_to_ack_ms)`, `mv_index` is exactly 1..6, each interval's end
does not precede its start, `submit_to_ack_ms` agrees with `ack_at_ms - submit_at_ms`, the makespan does not end before
the last ack, and `max_concurrent_engine` matches the per-view engine offsets.

## Offline fixtures

`fixtures/reports/<set>/` holds `state-guard-perf.json` and `state-guard-crash.json`, plus `run.json`
(`exit_codes` per suite, failure lines, and the JUnit `outcomes` map of `"<suite>: <test>"` to
`passed|failed|skipped`) and optional `changes.json` (candidate diff as
`[{"path", "status", "old_mode", "new_mode", "binary"}]`; absent means no changes). `fixtures/diffs/*.json` are policy
cases. Regenerate with `python3 experiments/darwin/fixtures/make_fixtures.py`.

## Promotion flow

Candidates live only in Darwin's local refs/workspaces until promoted. The coordinator reviews the accepted candidates,
integrates overlapping shared-file edits, re-runs the paired measurement on the integrated tree and lands everything in
the single PR (#108). Nothing in this directory pushes, commits or opens PRs.

## What was and was not verified

Verified offline: policy, report validation, scoring, the CLI end to end against synthetic fixtures, the runner against a
fake suite process, redaction, and config/manifest consistency. Verified on the devbox: the Darwin 0.3.0 wheel, Copilot CLI
1.0.67 and one real agent iteration with the GHE identity; the shape of the workspace and environment Darwin hands an
evaluator (a git repo with the candidate as HEAD, no remotes, env expansion); the crash gate on Spark 3.5 and 4.1 with
the Delta catalog backend: `StateGuardCrashRecoverySpec` plus the 11 named witness cases all pass, 19 gated points, zero
failing, and only `cdf/mv_version_and_cdf_watermark_gap` reported as the known gap. The JUnit reader was checked against
real ScalaTest XML. Not verified: absolute OneLake behaviour (the latency is modeled), models other than
`claude-haiku-4.5`, and the report fields the real suite emits beyond what `report.py` validates.
