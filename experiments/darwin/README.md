# Darwin experiments

Darwin evolves committed product code with an AI agent:
each iteration the agent edits allowlisted files, a trusted evaluator builds and measures the candidate,
and Darwin keeps the candidates that improve the evaluator's score. This directory holds everything a
campaign needs, so any OpenIVM-Spark optimization goal can be run the same way.

## How a campaign works

1. **Define the goal** in `manifest.json` (mirrored in `manifest.yaml`): lanes, the product files each
   lane may edit, frozen paths, correctness and crash gates, budgets and acceptance thresholds.
2. **Lock the evaluator.** `evaluator/` scores a candidate against a same-box baseline and prints `@RESULT`
   metrics and `@FEEDBACK` for the agent. `evaluator.lock.json` pins its digest, so candidates cannot edit
   the evaluator, tests, fixtures or scoring.
3. **Validate offline** with `bash experiments/darwin/validate-offline.sh` and
   `python3 experiments/darwin/evaluator/lock.py --verify`. No Spark, Darwin or network is needed.
4. **Provision the tools** with `experiments/darwin/provision-agent-tools.sh` (pinned Darwin wheel and
   Copilot CLI).
5. **Run one lane per checkout.** Point `darwin evolve` at a config in `configs/`, with the evaluator
   running from a separate trusted checkout of the frozen harness commit.
6. **Review and promote.** Candidates stay in local refs. Promote one only when every gate passes on
   both Spark targets; invalid candidates never get speed credit.

All Java, Scala, sbt and Spark work runs in the pinned devcontainer through Nx.

## Documentation

- [Evidence and layout](docs/OVERVIEW.md): maintained files, modeled-latency limits and historical qualification.
- [Running lanes](docs/RUNNING_LANES.md): pinned tools, trusted checkout, the sample config and deriving other lanes.
- [Evaluation](docs/EVALUATION.md): crash, correctness, incremental, concurrency and acceptance gates.
- [Lane ownership](docs/LANES.md): each lane's goal, allowed files and frozen boundaries.
- [Report schema](docs/REPORT_SCHEMA.md): perf, crash, witness and synthetic report contracts.
- [Offline validation](docs/OFFLINE_VALIDATION.md): positive and negative samples without committed reports.

The current manifest and `configs/lane-a.yaml` target concurrent-refresh state-guard contention; to start
a new campaign, change the manifest, evaluator and lane configs, then re-lock and re-validate.
