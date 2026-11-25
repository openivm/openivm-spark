# Darwin state-guard experiment

Trusted evaluator and lane configs for evolving OpenIVM-Spark product code (Scala) with Darwin so
that concurrent materialized-view (MV) refresh spends less time contending on the state guard.
Five independent lanes (A-E) each get their own Darwin run; a candidate is only credited when it is
correct, recovers from crashes, keeps every MV incremental and is measurably faster than a fresh
baseline measured on the same box.

## Evidence class (read first)

The suite measures **local modeled latency** (`evidence_class: local-relative-modeled-latency`).
It is relative evidence for ranking candidates on one box. It is **not** OneLake proof, and it does
not prove that Delta I/O is the cause of the contention. The measured spans are nested and
non-additive (catalog finalization runs inside the barrier hold, waits overlap), so a sum of spans
is not a critical path. Reference numbers from the earlier modeled run, kept in
`manifest.json` as `reference_evidence` for documentation and feedback only (never scored):

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
  evaluator/               state_guard_eval.py (CLI), darwin_evaluator.py (Darwin wrapper), policy, report, scoring, runner, lock
  configs/lane-{a..e}.yaml Darwin configs, one per lane
  fixtures/                synthetic report sets + diff cases; make_fixtures.py regenerates them (--check verifies)
  tests/                   unittest suite (offline, no Darwin, no Nx, no Spark)
  validate-offline.sh      runs the suite from the repo root
```

## Prerequisites

- A devbox with the repo's pinned devcontainer. Java, Scala, sbt, Spark and Delta only run there;
  the evaluator invokes the trusted suite through Nx and never uses host toolchains.
- `python3` 3.12 and `bash` on the host. The evaluator is stdlib only (no `uv`, no `pip install`).
- Darwin installed from its clone: `python scripts/xmake.py install` (installs the wheel and
  persists `COPILOT_CLI_PATH`). Set `COPILOT_CLI_PATH` explicitly if the Copilot CLI is elsewhere.
  Behaviour of a PyInstaller-built Darwin binary was not verified here.
- A GitHub/GHE token for the agent in the environment (`COPILOT_GITHUB_TOKEN` or `GITHUB_TOKEN`);
  never commit it. For GHE set `agents[0].auth.options.hostname`. The evaluator strips
  `*TOKEN*`/`*SECRET*`/`*PASSWORD*`/`*CREDENTIAL*`/`*API_KEY*` variables before running the suite and
  redacts token-looking text from everything it prints.
- The trusted suite from the harness work: Nx target `spark-ext:state-guard-perf`. It does not
  exist in the repo until the harness lands; scoring cannot run before then.

Optional environment:

| Variable | Default | Meaning |
| --- | --- | --- |
| `OPENIVM_DARWIN_EXEC_PREFIX` | empty (run Nx directly) | command prefix that enters the devcontainer, e.g. `npx --no-install nx run devcontainer:exec --` |
| `OPENIVM_DARWIN_CONTAINER_ROOT` | `/workspaces/openivm-spark` | where a workspace root appears inside the devcontainer (report dirs are translated to it). The container must bind the candidate and baseline worktrees at that path; that binding belongs to the harness/devbox setup, not to this evaluator |

## Offline validation

No Darwin, Nx, Spark or network needed:

```bash
bash experiments/darwin/validate-offline.sh          # = python3 -m unittest discover -s experiments/darwin/tests -t .
python3 experiments/darwin/fixtures/make_fixtures.py --check   # fixtures match the generator
python3 experiments/darwin/evaluator/lock.py --verify          # lock matches the evaluator files
```

The script is invoked through `bash` (the executable bit is not set). Tests write scratch files to
`experiments/darwin/.scratch/` (git-ignore it or delete it) and print only test status.

## Before the first run

Replace every `<...>` placeholder in the lane configs (`grep -n '<' experiments/darwin/configs/*.yaml`):

| Placeholder | Where | Value |
| --- | --- | --- |
| `<BASELINE_REF>` | `source.ref` | the committed baseline branch/sha Darwin evolves from |
| `<REPOSITORY_URL_OR_LOCAL_PATH>` | `source.options.repository` | the repo Darwin clones |
| `<DARWIN_RUNTIME_DIR>/containers` | `container.options.path` | scratch dir for Darwin workspaces |
| `<BASELINE_SHA>` | `evaluator.options.baseline_ref` | the SAME baseline commit the paired baseline runs use |
| `<SHA256_OF_evaluator.lock.json>` | `evaluator.options.lock_sha256` | output of `python3 experiments/darwin/evaluator/lock.py --digest` |
| `<MODEL_NAME>` | `agents[0].models[0].name` | the one place the agent model is named |

Optional `evaluator.options` (declared by `StateGuardOptions`): `spark_target` (`spark-3.5` default,
or `spark-4.1`), `pairs` (default 3, minimum 3), `baseline_repo`, `cache_dir`, `run_timeout_s`
(per suite run, default 1200), `deadline_s` (all runs, default 5000), `manifest`.

The baseline commit must be fetchable by the evaluator (`baseline_repo` if the workspace has no
remote). Darwin evaluation clones are shallow, so the baseline sha is fetched at depth 1.

## Trusted evaluator copy

Candidates edit a workspace copy of the repo. The evaluator must therefore run from a copy they
cannot touch:

1. Make a pinned checkout of the baseline commit outside the candidate workspaces, e.g.
   `git worktree add <TRUSTED_DIR> <BASELINE_SHA>`, and point `darwin run` at the config there.
   Darwin loads `evaluator.path` (`../evaluator/darwin_evaluator.py`) relative to the config
   directory, so the wrapper and CLI both come from the trusted copy.
2. Pin the lock digest: put `python3 experiments/darwin/evaluator/lock.py --digest` into
   `evaluator.options.lock_sha256`. The CLI recomputes the digest of `evaluator/*.py` and
   `manifest.json` against `evaluator.lock.json` and against the pinned value; a mismatch scores 0.0
   with `trust_gate=0`.
3. Defense in depth: any candidate diff under `experiments/**` is a frozen-file violation (0.0).

After ANY edit to `evaluator/*.py` or `manifest.json`, regenerate the lock and re-pin it:
`python3 experiments/darwin/evaluator/lock.py --write`, then update `lock_sha256` in the configs.

## Running a lane

Run ONE lane per Darwin daemon (Darwin loads every evaluator as the module `evaluation_module`, so
two lanes in one daemon collide):

```bash
darwin start --background
darwin run experiments/darwin/configs/lane-a.yaml     # likewise lane-b .. lane-e, each on its own daemon
```

Each lane: `executor.termination.max_iterations: 18`; early stop after 8 iterations without
improvement of `combined_score` (absolute `min_improvement: 0.5`, since the score spans roughly
0-175); evaluator timeout 5400 s per iteration, no retries. Darwin's `darwin evolve <file>`
experiment format (commands as plain strings) is **not** implemented here; the configs use
`darwin run` with the Python evaluator.

## Manual scoring

Candidate (paired against a fresh same-box baseline, `--pairs >= 3`, alternating baseline-first and
candidate-first):

```bash
python3 experiments/darwin/evaluator/state_guard_eval.py --mode candidate \
  --workspace <candidate-checkout> --baseline-ref <sha> --lane a --pairs 3 --spark-target spark-3.5 \
  --lock-sha256 "$(python3 experiments/darwin/evaluator/lock.py --digest)"
```

Baseline sanity check (runs the suite once, always prints `combined_score=0.000000`):
`--mode baseline --workspace <checkout> --lane a`. Offline: add `--offline-fixtures
experiments/darwin/fixtures/reports --offline-candidate-set good-candidate --skip-lock-check`.

Exit code 0 for every candidate outcome (invalid scores 0.0); exit 2 only for evaluator
infrastructure errors, still with `combined_score=0.000000`. `darwin_evaluator.py` converts timeouts,
crashes and exit 2 into a zero-score result with `infra_error=1`; it never raises into Darwin.

### Output protocol (our own, not Darwin's)

stdout carries `@FEEDBACK <one line>` lines (feedback is capped per line) followed by
`@RESULT <name>=<finite float>`. The wrapper turns `@RESULT` into `EvaluationResult.metrics`
(every value finite and clamped to +-1e12; a non-1.0 `*_gate` forces `combined_score` to 0.0) and
`@FEEDBACK` into a `feedback` string artifact the agent reads. Progress logs go to stderr.

`@RESULT` names: `combined_score`; gates `allowlist_gate`, `correctness_gate`, `crash_gate`,
`incremental_gate`, `concurrency_gate`, `report_gate`, `trust_gate` (1.0 pass, 0.0 fail or not
evaluated); `wait_reduction`; `e2e_improvement_median` (+ `_min`, `_max`); `baseline_targeted_wait_ms`,
`candidate_targeted_wait_ms`; `baseline_e2e_ms_median`, `candidate_e2e_ms_median`, `makespan_ms`,
`baseline_e2e_spread`; `accepted` (0/1); `pairs_completed`; `shared_files_touched`;
`changed_file_count`; engine-overlap and `max_concurrent_engine` medians; lane informational
`info_*_baseline|candidate`; and per MV `mvN_submit_to_ack_ms`, `mvN_state_barrier_hold_ms`,
`mvN_state_barrier_wait_ms`, `mvN_capture_wait_ms`, `mvN_publication_queue_wait_ms` (N = 1..6).

## Scoring and acceptance

- Invalid (any gate false, allowlist violation, build/test failure, non-finite or missing metrics,
  timeout, trust mismatch) => `combined_score = 0.0`, with `@FEEDBACK` naming the gate and the first
  failure lines.
- Valid: `combined = max(0.001, 100*clamp(e2e_improvement_median,-1,1) + 50*clamp(wait_reduction,-1,1) + (25 if accepted))`.
  The 0.001 floor keeps a valid-but-slower candidate distinguishable from an invalid 0.0.
- `e2e_improvement_median` = median over pairs of `(baseline_makespan - candidate_makespan) / baseline_makespan`
  (`makespan_ms` == `end_to_end_ms`, includes the final async drain).
- `wait_reduction` = `1 - candidate_targeted / baseline_targeted`, where each side is the median over its
  runs of the lane's targeted total (table below).
- **Accepted** only if all gates pass AND `wait_reduction >= 0.80` AND `e2e_improvement_median >= 0.10`.
- Anti-gaming gates: `max_concurrent_engine >= baseline median - 1`; `serial_sum_ms` may not fall
  below 0.5 x baseline while makespan rises (so serializing or deferring work outside the measured
  interval cannot win); `full_refresh_count == 0` and `incremental_count == 6` (an incrementalizable
  MV must never fall back to FULL_REFRESH).
- Crash gate: at least 3 crash points; every point with `known_gap == false` must have `crashed`,
  `recovered`, `parity`, `metadata_converged` and `incremental_after_recovery` all true. Points with
  `known_gap == true` never fail the gate and are echoed as "documented known gap:
  MV-version/CDF-watermark".

## Lanes and ownership

Prefix `CM` = `spark-ext/ivm-common/src/main/scala/org/openivm/spark/common/`, `RDB` = `CM/rocksdb/`,
`MVC` = `spark-ext/ivm-extension/src/main/scala/org/openivm/spark/commands/MaterializedViewCommands.scala`.
`manifest.json` holds the authoritative lists; the evaluator allows owned + shared + `new_files` for the
lane and emits a warning when a shared file is touched (the coordinator integrates overlapping edits).

| Lane | Goal | Targeted waits (sum over 6 MVs) | Owned | Shared | New files |
| --- | --- | --- | --- | --- | --- |
| A | shared barrier / lock scope | `state_barrier_wait_ms` + `capture_wait_ms` | `RDB/OpenIvmStateBarrier.scala`, `RDB/OpenIvmRocksDBRegistry.scala` | MVC | none |
| B | catalog finalization | `catalog_finalize_ms` | `CM/MvCatalog.scala`, `CM/CdfWatermarkBackend.scala`, `CM/StagingCatalog.scala` | MVC, `CM/DeltaMvCatalogBackend.scala` | none |
| C | per-MV publication / queue | `publication_queue_wait_ms` + `publication_ms` | `RDB/OpenIvmStateSync.scala` | MVC, `RDB/OpenIvmStateBarrier.scala` | none |
| D | whole-path efficiency + catalog backend | `wall_ms` (catalog `fs_ops` total informational) | `CM/MvCatalogBackend.scala`, `CM/FeatureGate.scala` | MVC, `CM/DeltaMvCatalogBackend.scala` | none |
| E | incremental state shipping | `counts.state_sync.bytes_uploaded` (state-sync write ops informational) | none | `RDB/OpenIvmStateSync.scala` | `RDB/OpenIvmStateDelta*.scala`, `RDB/OpenIvmStateSyncCodec*.scala` |

`OpenIvmStateDelta*` and `OpenIvmStateSyncCodec*` do not exist in the repo today; lane E is the only
lane allowed to add files.

Frozen for every lane (any diff vs the baseline => 0.0): `experiments/**`, `spark-ext/**/src/test/**`,
`spark-ext/build.sbt`, `spark-ext/project/**`, `spark-ext/dev/**`, `spark-ext/project.json`,
`.github/**`, `project.json`, `nx.json`, `package.json`, `package-lock.json`, `.devcontainer/**`,
`contrib/**`, telemetry/measurement files (`**/telemetry/**`, `OpenIvmExecutionSpan.scala`,
`RefreshProfile.scala`, `PhaseTimeline.scala`) and any `*Perf*Spec*`, `state-guard*` or
`.gitattributes` path. Also rejected: deleted files, files outside the lane allowlist, symlinks,
binary blobs and file-mode changes. The changed set is `git diff --name-status --no-renames <baseline>`
plus `git ls-files --others --exclude-standard`; Darwin's `output/*.json|md` protocol files are
ignored.

## Known gap

A crash between the MV-version write and the CDF-watermark write is a documented known gap
(`MV-version/CDF-watermark`). It never fails the crash gate and is always echoed in feedback. It
only becomes relevant when a candidate changes that path, which lanes B and D may do; the prompts
for those lanes say so.

## Promotion flow

Candidates live only in Darwin's local refs/workspaces until promoted. The coordinator reviews the
accepted candidates, integrates overlapping shared-file edits, re-runs the paired measurement on the
integrated tree and lands everything in a single PR (#108). Nothing in this directory pushes,
commits or opens PRs.

## What was and was not verified

Verified offline: policy, report validation, scoring, CLI end to end against synthetic fixtures,
the runner against a fake suite process, wrapper parsing, redaction, and config/manifest
consistency (`bash experiments/darwin/validate-offline.sh`). Not verified: any run with Darwin
itself (the configs and wrapper were checked against Darwin's source only), the Nx
`state-guard-perf` target, the devcontainer binding, PyInstaller-binary behaviour, and the report
fields the real suite emits beyond the schema this evaluator validates. The `good-candidate`
fixtures are synthetic. The failure-line regex and the `FULL_REFRESH` vocabulary used on `outcome`
are best guesses until the suite exists.

## Report schema (`state-guard-perf.json`, `openivm.state-guard-perf/v1`)

Written by the suite into `$OPENIVM_STATE_GUARD_REPORT_DIR`. All numbers finite and `>= 0`; unknown
`evidence_class` => invalid.

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
`{"points": [{"name", "crashed", "recovered", "parity", "metadata_converged", "incremental_after_recovery", "known_gap", "notes"}]}`.
`evaluator/report.py` is the validator and treat it as the exact contract. Beyond field types it checks
that `end_to_end_ms == makespan_ms`, `serial_sum_ms == sum(submit_to_ack_ms)`, `mv_index` is exactly
1..6, each interval's end does not precede its start, `submit_to_ack_ms` agrees with
`ack_at_ms - submit_at_ms`, the makespan does not end before the last ack, and `max_concurrent_engine`
matches the per-view engine offsets. `engine_overlap_ms` and `barrier_hold_overlap_ms` are validated
as numbers but not recomputed.

## Offline fixtures

`fixtures/reports/<set>/` holds `state-guard-perf.json` and `state-guard-crash.json`, plus optional
`run.json` (`exit_code`, failure lines) and `changes.json` (candidate diff as
`[{"path", "status", "old_mode", "new_mode", "binary"}]`; absent means no changes). Sets: `baseline`,
`good-candidate`, `lane-a-only`, `slow-valid`, `bad-parity`, `bad-crash`, `full-refresh-regression`,
`serialized-engines`, `deferred-work`, `nonfinite`, `wrong-evidence-class`, `bad-allowlist`.
`fixtures/diffs/*.json` are policy cases. Regenerate with `python3 experiments/darwin/fixtures/make_fixtures.py`.
