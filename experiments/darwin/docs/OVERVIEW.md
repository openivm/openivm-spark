# Evidence and experiment layout

[Darwin experiment](../README.md)

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

```text
experiments/darwin/
  README.md                topic index
  docs/                    operating guides and report contract
  manifest.json            authoritative maintained policy, gates, budgets and lane targets
  manifest.yaml            maintained human mirror; not parsed by the evaluator
  evaluator.lock.json      integrity hashes of evaluator/*.py and manifest.json
  evaluator/               trusted CLI, policy, report, scoring, runner and lock code
  configs/lane-{a..e}.yaml  five maintained, credential-free Darwin evolve examples
  fixtures/make_fixtures.py deterministic synthetic report generator
  fixtures/diffs/          maintained policy inputs and expected verdicts
  tests/                   offline unittest suite
  provision-agent-tools.sh pinned agent-tool provisioning
  validate-offline.sh      offline test entry point
  .scratch/                ignored generated reports, local configs and test scratch
```

The [manifest](../manifest.json) is the machine-read policy; its
[YAML mirror](../manifest.yaml) is for review. The
[offline guide](OFFLINE_VALIDATION.md) distinguishes maintained inputs from
disposable generated evidence. No generated report is release or performance evidence.

## Promotion flow

Candidates live only in Darwin's local refs/workspaces until promoted. The coordinator reviews the accepted candidates,
integrates overlapping shared-file edits, re-runs the paired measurement on the integrated tree and lands everything in
the single PR (#108). Nothing in this directory pushes, commits or opens PRs.

## Historical qualification

These are the original harness qualification notes, not a claim that the current
checkout or a new candidate has passed live qualification.

Verified offline: policy, report validation, scoring, the CLI end to end against synthetic fixtures, the runner against a
fake suite process, redaction, and config/manifest consistency. Verified on the devbox: the Darwin 0.3.0 wheel, Copilot CLI
1.0.67 and one real agent iteration with the GHE identity; the shape of the workspace and environment Darwin hands an
evaluator (a git repo with the candidate as HEAD, no remotes, env expansion); the crash gate on Spark 3.5 and 4.1 with
the Delta catalog backend: `StateGuardCrashRecoverySpec` plus the 11 named witness cases all pass, 19 gated points, zero
failing, and only `cdf/mv_version_and_cdf_watermark_gap` reported as the known gap. The JUnit reader was checked against
real ScalaTest XML. Not verified: absolute OneLake behaviour (the latency is modeled), models other than
`claude-haiku-4.5`, and the report fields the real suite emits beyond what `report.py` validates.
