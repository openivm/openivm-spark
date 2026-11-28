# Offline validation and sample hygiene

[Darwin experiment](../README.md)

The evaluator tests need Python 3.12 and Bash, not Darwin, Spark or a
network connection. Run them from the repository root in the pinned devcontainer.
PyYAML, when already available, also validates the YAML schema of every lane config
present; without it, that one optional test is reported as skipped.

## Maintained inputs versus generated reports

Keep these small, reviewable inputs in Git:

- `configs/lane-a.yaml` is the maintained end-to-end Darwin sample, with the exact
  experiment name, trusted evaluator arguments, model/budget pins, metric lookups
  and lane prompt. [Running lanes](RUNNING_LANES.md) derives lanes B–E from it.
- `manifest.json` owns policy, witness identities, budgets, gates and thresholds;
  `manifest.yaml` is its maintained human mirror.
- `evaluator.lock.json` is a required integrity artifact, not a performance
  report; it locks `evaluator/*.py` and `manifest.json`.
- `fixtures/make_fixtures.py` defines the synthetic report scenarios.
- `tests/test_policy.py` holds the policy cases and expected verdicts inline.

Generate reports instead of committing JSON snapshots. The generator defaults
to `experiments/darwin/.scratch/fixtures/reports/`; tests create an isolated
scratch fixture tree per process and remove it on exit. Local lane configs,
Darwin homes, raw outputs and caches stay untracked: the directory `.gitignore`
is an allowlist of the harness, its docs and tests, `configs/lane-a.yaml` and
`fixtures/make_fixtures.py`, and it also rejects `output/`, `outputs/`,
`reports/`, `runs/` and `artifacts/` at any depth. Adding a tracked file
outside that allowlist needs a reviewed `.gitignore` change. Ignoring a file does
not make it an allowed candidate edit: the evaluator's frozen-path and lane
policies still apply.

## Run the offline suite

```bash
bash experiments/darwin/validate-offline.sh
python3 experiments/darwin/evaluator/lock.py --verify
```

The script runs `python3 -m unittest discover -s experiments/darwin/tests -t .`.
It materializes fixtures automatically and prints test status, not benchmark
logs. No generated fixture JSON needs to be present in the checkout first.

## Run the CLI end to end

This sample generates all scenarios in ignored scratch, checks reproducibility,
verifies the evaluator lock and scores the positive sample with that digest:

```bash
python3 experiments/darwin/fixtures/make_fixtures.py
python3 experiments/darwin/fixtures/make_fixtures.py --check
python3 experiments/darwin/evaluator/lock.py --verify
lock_sha256="$(python3 experiments/darwin/evaluator/lock.py --digest)"
python3 experiments/darwin/evaluator/state_guard_eval.py \
  --mode candidate --workspace "$PWD" --lane a --pairs 3 --spark-target spark-3.5 \
  --offline-fixtures experiments/darwin/.scratch/fixtures/reports \
  --offline-candidate-set good-candidate --lock-sha256 "$lock_sha256"
```

The sample must report `accepted=1.000000`, `recovery_qualified=1.000000` and
three completed pairs. These are **synthetic gate results**, not speed evidence.
To use another scratch root, pass `--output-dir <root>` to generation and
`--check`, then pass `<root>/reports` to the evaluator.
Offline mode skips Nx and Git; real campaigns must use the full frozen harness
SHA and separate trusted checkout from [running lanes](RUNNING_LANES.md).

Repeat the CLI command with a scenario below. An invalid candidate normally
returns exit code 0 with a zero score and the failed gate in `@RESULT`; exit
code 2 means an evaluator infrastructure error, not a candidate verdict.

## Negative oracles and acceptance gates

| Scenario | Expected result |
| --- | --- |
| `bad-allowlist` | Allowlist gate fails before any timed pair. |
| `bad-parity` | Correctness gate fails and retains the first failure lines. |
| `bad-crash` | Crash gate fails before timed pairs. |
| `crash-point-dropped` | Missing gated crash coverage fails. |
| `witness-failed` | The named failed witness fails the crash gate. |
| `witness-not-run` | Missing and skipped witnesses are failures, not passes. |
| `full-refresh-regression` | Incremental gate fails; no speed credit. |
| `nonfinite` | Report gate rejects non-finite metrics. |
| `wrong-evidence-class` | Report gate rejects the unsupported evidence class. |
| `serialized-engines` | Concurrency gate rejects serialized execution. |
| `deferred-work` | Concurrency gate rejects work outside the acknowledged interval. |
| `unfixed-recovery` | Baseline-relative crash gate can pass, but recovery remains unqualified and acceptance is false. |
| `slow-valid` | Valid but slower result keeps the score floor, not acceptance. |

`baseline`, `good-candidate` and `lane-a-only` complete the 16 generated sets.
The baseline's deliberate stale-snapshot failure tests regression accounting;
it is not a claim about the current product. The inline policy cases separately retain
cross-lane/frozen-path, new-file, deletion, symlink, mode-change, binary,
path-traversal and protocol-file cases. The suite also retains timeout,
tampered-lock, redaction, malformed-report, known-gap misuse and runner safety
tests. No scoring thresholds or evaluator algorithms change when reports move
to scratch; see [evaluation](EVALUATION.md) for the acceptance formula.
