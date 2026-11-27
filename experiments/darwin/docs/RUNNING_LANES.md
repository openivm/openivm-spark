# Running Darwin lanes

[Darwin experiment](../README.md)

Run commands from the repository root inside the pinned devcontainer unless stated otherwise.

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
`COPILOT_CLI_PATH` exports to use. Use the provisioning script rather than installing unpinned tool versions.

**Identity.** Darwin reads `COPILOT_GITHUB_TOKEN`, `COPILOT_GH_HOST` and `COPILOT_GH_ACCOUNT` (and
`COPILOT_CLI_PATH`) from an env file. On a devbox that file is the repo's gitignored `.env`, delivered by
repo-bootstrapper's `sync/<repo>` folder; never put these values in job code, configs or logs. Use the GitHub host and account authorized for your environment.
The agent model in every lane config is `claude-haiku-4.5`, the model proven end to end in the preflight;
use a local copy of a lane config to try another model. The evaluator strips `*TOKEN*`, `*SECRET*`,
`*PASSWORD*`, `*CREDENTIAL*` and `*API_KEY*` variables before running the suite and redacts token-looking
text from everything it prints.

## Trusted evaluator checkout

Candidates edit a workspace copy of the repo, so the evaluator must run from a copy they cannot touch.
Make one pinned checkout of the **frozen harness commit** outside the lane checkouts, and use that SHA
as the baseline too:

```bash
# Run from the approved frozen harness commit, before creating lane checkouts.
export OPENIVM_DARWIN_BASELINE_SHA="$(git rev-parse HEAD)"
mkdir -p .scratch
git worktree add --detach .scratch/darwin-trusted "$OPENIVM_DARWIN_BASELINE_SHA"
export OPENIVM_DARWIN_TRUSTED="$(pwd)/.scratch/darwin-trusted"
export OPENIVM_DARWIN_LOCK_SHA256=$(python3 "$OPENIVM_DARWIN_TRUSTED/experiments/darwin/evaluator/lock.py" --digest)
```

The lane configs call `$OPENIVM_DARWIN_TRUSTED/experiments/darwin/evaluator/state_guard_eval.py`, which
recomputes the digest of its own `evaluator/*.py` + `manifest.json` against `evaluator.lock.json` and the
pinned `OPENIVM_DARWIN_LOCK_SHA256`; a mismatch scores 0.0 with `trust_gate=0`. Any candidate diff under
`experiments/**` is also a frozen-file violation. After ANY edit to `evaluator/*.py` or `manifest.json`, run
`python3 experiments/darwin/evaluator/lock.py --write` and re-pin the digest.

The workspace Darwin hands the evaluator is a git repo with the candidate as HEAD and **no remotes**, so
the configs pass `--baseline-repo "$OPENIVM_DARWIN_TRUSTED"` and the evaluator fetches the baseline SHA from there.

Record the full harness SHA, evaluator-lock digest, lane and Spark target with
each run. The lock covers `evaluator/*.py` and `manifest.json`, not documentation,
configs, tests or fixtures. Review scaffolding changes independently, rerun the
offline suite and `lock.py --verify`, and re-pin the harness commit before a
new campaign; do not infer integrity merely because a document moved.

## Running a lane

One lane per branch checkout (Darwin evolves committed HEAD; candidates stay in local refs until promoted):

```bash
cd <lane checkout>        # contains the frozen harness commit
mkdir -p .scratch
darwin evolve experiments/darwin/configs/lane-a.yaml \
  --home "$PWD/.scratch/darwin-home-a" --env-file /workspaces/openivm-spark/.env --repo .
```

Only `configs/lane-a.yaml` is tracked; it is the end-to-end sample. To run lane `<x>` (b–e), copy it to the
ignored `configs/lane-<x>.yaml` and change these, following [lane ownership](LANES.md) and `manifest.json`
`lanes.<x>`:

- the header title, the experiment name `openivm-state-guard-lane-<x>` and the evaluator's `--lane <x>`;
- the prompt's lane title, goal and target-metric wording;
- the editable-files list, which must equal that lane's `owned`, `shared` and `new_files` entries;
- the known-gap sentence "This lane may touch that path", kept only for lanes in `known_gap.lanes_that_may_touch`;
- the metrics block, which must declare exactly the names `state_guard_eval.Output(manifest, "<x>")` emits.

`validate-offline.sh` checks every config present, tracked or not, so run it before `darwin evolve` with
`--home "$PWD/.scratch/darwin-home-<x>"`. The R8 campaign's exact lane configs remain in branch history:

```bash
path=experiments/darwin/configs/lane-b.yaml
git show "$(git log --diff-filter=D -1 --format=%H -- "$path")^:$path"
```

Each lane: `max_iterations: 18`, early stop after 8 iterations without
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

## Manual scoring

```bash
python3 "$OPENIVM_DARWIN_TRUSTED/experiments/darwin/evaluator/state_guard_eval.py" --mode candidate \
  --workspace <candidate-checkout> --baseline-repo "$OPENIVM_DARWIN_TRUSTED" \
  --baseline-ref "$OPENIVM_DARWIN_BASELINE_SHA" --lane a --pairs 3 --spark-target spark-3.5 \
  --lock-sha256 "$OPENIVM_DARWIN_LOCK_SHA256"
```

Baseline sanity check (crash suite once, then K perf runs; always prints `combined_score=0.000000`):
`--mode baseline --workspace <checkout> --lane a`. For a complete offline example with generated reports, see
[offline validation](OFFLINE_VALIDATION.md#run-the-cli-end-to-end). Exit code 0 for every candidate outcome (invalid scores 0.0);
exit 2 only for evaluator infrastructure errors, still with `combined_score=0.000000`. `--spark-target spark-4.1` selects
the Spark 4.1 / Delta 4.2 target; both targets use the same discovery and the same suites.

The suites and their report contracts (`state-guard-perf.json`, `state-guard-crash.json`, `state-guard-events.jsonl`) are
written to `<root>/.logs/state-guard/<label>/` (`OPENIVM_STATE_GUARD_REPORT_DIR`). `OPENIVM_STATE_GUARD_SUITE`
(`all|perf|crash`) selects which suite the Nx target runs; `OPENIVM_STATE_GUARD_CATALOG_BACKEND` (`delta|rocksdb`) selects the
catalog backend of the perf suite.

## Suite entry point

Use the existing Nx target inside the pinned devcontainer:

```bash
npx --no-install nx run spark-ext:state-guard-perf --configuration=spark-3.5
```

Use `--configuration=spark-4.1` for the other runtime. This is a local suite
entry point, not a separate workflow dispatch. The evaluator owns the crash
gate and interleaved timed pairs; invoking the target alone does not score or
accept a candidate. See [evaluation](EVALUATION.md) for those gates.
