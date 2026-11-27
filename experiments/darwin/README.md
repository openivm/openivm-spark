# Darwin state-guard experiment

Trusted evaluator and lane configs for evolving OpenIVM-Spark product code (Scala) with Darwin so
that concurrent materialized-view (MV) refresh spends less time contending on the state guard.
Five independent lanes (A-E) each get their own `darwin evolve` run. A candidate is only credited
when the crash-recovery and correctness gates pass, every MV stays incremental, and it is measurably
faster than a fresh baseline measured on the same box.

## Documentation

- [Evidence and layout](docs/OVERVIEW.md) explains modeled-latency limits, maintained files and historical qualification.
- [Running lanes](docs/RUNNING_LANES.md) covers pinned tools, trusted evaluator identities, the lane-A sample config and how to derive lanes B–E.
- [Evaluation](docs/EVALUATION.md) defines crash, correctness, incremental, concurrency and acceptance gates.
- [Lane ownership](docs/LANES.md) lists each lane's goals, allowed files and frozen boundaries.
- [Report schema](docs/REPORT_SCHEMA.md) describes perf, crash, witness and synthetic report contracts.
- [Offline validation](docs/OFFLINE_VALIDATION.md) runs the positive and negative samples without committing generated reports.
