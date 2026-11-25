#!/usr/bin/env python3
"""Trusted state-guard evaluator CLI.

Scores a candidate workspace against a fresh same-box baseline using interleaved pairs of
the trusted `state-guard-perf` suite. Prints `@FEEDBACK <text>` and `@RESULT <name>=<float>`
lines on stdout; progress goes to stderr. Exit 0 for any candidate outcome (invalid => score
0.0), 2 for evaluator infrastructure errors.
"""

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Any, TextIO

import lock
import policy
import report
import runner
import scoring

GATES = (
    "allowlist_gate",
    "correctness_gate",
    "crash_gate",
    "incremental_gate",
    "concurrency_gate",
    "report_gate",
    "trust_gate",
)
SCALARS = (
    "combined_score",
    *GATES,
    "wait_reduction",
    "e2e_improvement_median",
    "e2e_improvement_min",
    "e2e_improvement_max",
    "baseline_targeted_wait_ms",
    "candidate_targeted_wait_ms",
    "baseline_e2e_ms_median",
    "candidate_e2e_ms_median",
    "makespan_ms",
    "baseline_e2e_spread",
    "accepted",
    "pairs_completed",
    "shared_files_touched",
    "changed_file_count",
)
FEEDBACK_LIMIT = 600
REMINDER = (
    "reminder: spans are nested and non-additive (catalog finalization runs inside the barrier hold, waits overlap); "
    "Delta I/O is NOT proven to be the cause; evidence is local modeled latency (relative only, not OneLake proof); "
    "an incrementalizable MV must never fall back to FULL_REFRESH; keep crash-safety and ordering of state transitions"
)


def log(message: str) -> None:
    print(f"[state-guard-eval] {message}", file=sys.stderr, flush=True)


class Output:
    def __init__(self, manifest: dict[str, Any], lane: str) -> None:
        self.names = list(SCALARS) + scoring.metric_names(lane, manifest)
        self.results = {name: 0.0 for name in self.names}
        self.feedback: list[str] = []
        self.gates: dict[str, bool | None] = {g: None for g in GATES}

    def say(self, text: str) -> None:
        self.feedback.append(" ".join(str(text).split())[:FEEDBACK_LIMIT])

    def mark(self, gate: str, ok: bool) -> None:
        current = self.gates[gate]
        self.gates[gate] = ok if current is None else (current and ok)

    def failed(self) -> bool:
        return any(value is False for value in self.gates.values())

    def emit(self, stream: TextIO) -> None:
        for gate, value in self.gates.items():
            self.results[gate] = 1.0 if value else 0.0
        for line in self.feedback:
            print(f"@FEEDBACK {runner.redact(line)}", file=stream)
        for name in self.names:
            print(f"@RESULT {name}={scoring.sane(self.results[name]):.6f}", file=stream)
        stream.flush()

    def gate_summary(self) -> str:
        label = {True: "PASS", False: "FAIL", None: "NOT EVALUATED"}
        return "gates: " + ", ".join(f"{g.removesuffix('_gate')}={label[v]}" for g, v in self.gates.items())


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--mode", choices=("baseline", "candidate"), required=True)
    parser.add_argument("--workspace", required=True, help="candidate repo root (baseline mode: any checkout of the repo)")
    parser.add_argument("--baseline-ref", default="", help="baseline commit/ref; required unless --offline-fixtures")
    parser.add_argument("--baseline-repo", default="", help="repository to fetch the baseline ref from")
    parser.add_argument("--lane", choices=("a", "b", "c", "d", "e"), required=True)
    parser.add_argument("--pairs", type=int, default=None)
    parser.add_argument("--spark-target", default=None)
    parser.add_argument("--cache-dir", default=os.path.join("~", ".cache", "openivm-darwin"))
    parser.add_argument("--manifest", default=None)
    parser.add_argument("--run-timeout", type=float, default=None, help="seconds per suite run")
    parser.add_argument("--deadline", type=float, default=0.0, help="total seconds for all runs (0 = none)")
    parser.add_argument("--no-box-lock", action="store_true")
    parser.add_argument("--lock-sha256", default="", help="pinned sha256 of evaluator.lock.json")
    parser.add_argument("--skip-lock-check", action="store_true", help="development only; never use in Darwin configs")
    parser.add_argument("--offline-fixtures", default="", help="directory of fixture report sets; skips Nx and git")
    parser.add_argument("--offline-baseline-set", default="baseline")
    parser.add_argument("--offline-candidate-set", default="good-candidate")
    return parser.parse_args(argv)


def pct(value: float) -> str:
    return f"{value * 100:.1f}%"


class Session:
    """One evaluation: shared state for the pair loop."""

    def __init__(self, args: argparse.Namespace, manifest: dict[str, Any], run: Any, out: Output) -> None:
        self.args, self.manifest, self.runner, self.out = args, manifest, run, out
        self.started = time.monotonic()
        self.run_count = 0

    def remaining(self) -> float:
        if self.args.deadline <= 0:
            return float("inf")
        return self.args.deadline - (time.monotonic() - self.started)

    def run_suite(self, role: str, root: Path, pair: int) -> runner.RunResult:
        timeout = min(self.args.run_timeout, self.remaining())
        if timeout <= 0:
            return runner.RunResult(exit_code=None, timed_out=True)
        self.run_count += 1
        label = f"{role}-{os.getpid()}-{pair}"
        log(f"pair {pair + 1}/{self.args.pairs} {role} run starting (timeout {timeout:.0f}s)")
        result = self.runner.run(role, root, label, self.args.spark_target, timeout)
        log(f"pair {pair + 1}/{self.args.pairs} {role} run finished: exit={result.exit_code} timed_out={result.timed_out} {result.duration_s:.0f}s")
        return result

    def check_baseline(self, result: runner.RunResult, pair: int) -> dict[str, Any]:
        where = f"baseline run {pair + 1}"
        if result.timed_out:
            raise runner.InfraError(f"{where} timed out")
        if result.exit_code != 0:
            raise runner.InfraError(f"{where} failed (exit {result.exit_code}): {' | '.join(result.failure_excerpt(4))}")
        errors = [e for e in result.read_errors if e.startswith(self.manifest["nx"]["perf_report"])]
        if errors or result.perf is None:
            raise runner.InfraError(f"{where}: {'; '.join(errors) or 'no perf report'}")
        problems = report.validate_perf(result.perf, self.args.spark_target, self.manifest)
        if problems:
            raise runner.InfraError(f"{where}: invalid perf report: {'; '.join(problems[:4])}")
        gate_failures = report.perf_gate_failures(result.perf, self.manifest)
        broken = [f for fails in gate_failures.values() for f in fails]
        if broken:
            raise runner.InfraError(f"{where}: baseline itself is unhealthy: {'; '.join(broken[:4])}")
        return result.perf

    def check_candidate(self, result: runner.RunResult, pair: int) -> dict[str, Any] | None:
        out, manifest = self.out, self.manifest
        perf_name, crash_name = manifest["nx"]["perf_report"], manifest["nx"]["crash_report"]
        if result.timed_out:
            out.mark("correctness_gate", False)
            out.say(f"candidate run {pair + 1} timed out or the evaluation deadline was reached")
            return None
        if result.exit_code != 0:
            out.mark("correctness_gate", False)
            out.say(f"suite exited {result.exit_code} (a correctness/recovery assertion or the build failed)")
            for line in result.failure_excerpt():
                out.say(f"failure: {line}")
        perf_ok = False
        if result.perf is None:
            if result.exit_code == 0:
                out.mark("report_gate", False)
                out.say(next((e for e in result.read_errors if e.startswith(perf_name)), f"{perf_name}: not written"))
        else:
            problems = report.validate_perf(result.perf, self.args.spark_target, manifest)
            if problems:
                out.mark("report_gate", False)
                out.say(f"invalid {perf_name}: " + "; ".join(problems[:6]))
            else:
                perf_ok = True
        self._check_crash(result, crash_name)
        if not perf_ok:
            return None
        failures = report.perf_gate_failures(result.perf, manifest)
        out.mark("correctness_gate", not failures["correctness"] and result.exit_code == 0)
        out.mark("incremental_gate", not failures["incremental"])
        for gate, label in (("correctness", "correctness"), ("incremental", "incremental")):
            for failure in failures[gate][:6]:
                out.say(f"{label} gate: {failure}")
        if out.failed() or result.exit_code != 0:
            return None
        out.mark("report_gate", True)
        return result.perf

    def _check_crash(self, result: runner.RunResult, crash_name: str) -> None:
        out = self.out
        if result.crash is None:
            if result.exit_code == 0:
                out.mark("crash_gate", False)
                out.say(next((e for e in result.read_errors if e.startswith(crash_name)), f"{crash_name}: not written"))
            return
        problems = report.validate_crash(result.crash, self.manifest)
        if problems:
            out.mark("crash_gate", False)
            out.say(f"invalid {crash_name}: " + "; ".join(problems[:6]))
            return
        failures, known = report.crash_gate(result.crash)
        out.mark("crash_gate", not failures)
        for failure in failures[:6]:
            out.say(f"crash gate: {failure}")
        for name in known:
            out.say(f"{self.manifest['known_gap']['text']} [point {name}]")


def baseline_sources(args: argparse.Namespace) -> list[str]:
    sources = [args.baseline_repo, str(Path(args.workspace).resolve())]
    rc, origin = runner.run_git(args.workspace, "remote", "get-url", "origin")
    if rc == 0:
        sources.append(origin.decode().strip())
    return [s for s in sources if s]


def prepare_roots(args: argparse.Namespace, session: Session) -> tuple[Path, list[dict[str, Any]]]:
    """Return (baseline root, candidate allowlist changes)."""
    out, manifest = session.out, session.manifest
    if args.offline_fixtures:
        changes_file = Path(args.offline_fixtures) / args.offline_candidate_set / "changes.json"
        changes = report.load_json(changes_file) if changes_file.is_file() else []
        return Path(args.workspace), changes
    if not args.baseline_ref:
        raise runner.InfraError("--baseline-ref is required")
    sha = policy.resolve_baseline(args.workspace, args.baseline_ref, args.baseline_repo or None)
    log(f"baseline {args.baseline_ref} -> {sha}")
    changes = policy.collect_changes(args.workspace, sha) if args.mode == "candidate" else []
    if args.mode == "candidate":
        verdict = policy.check_changes(changes, manifest, args.lane)
        if not verdict["ok"]:
            return Path(args.workspace), changes
    root = runner.prepare_baseline_worktree(Path(args.cache_dir).expanduser(), sha, baseline_sources(args))
    return root, changes


def evaluate_candidate(args: argparse.Namespace, session: Session) -> None:
    out, manifest = session.out, session.manifest
    lane = manifest["lanes"][args.lane]
    candidate_root = Path(args.workspace)
    if args.pairs < manifest["acceptance"]["min_pairs"]:
        raise runner.InfraError(f"--pairs must be >= {manifest['acceptance']['min_pairs']} for candidate scoring")

    baseline_root, changes = prepare_roots(args, session)
    verdict = policy.check_changes(changes, manifest, args.lane)
    out.mark("allowlist_gate", verdict["ok"])
    out.results["changed_file_count"] = len(verdict["changed"])
    out.results["shared_files_touched"] = verdict["shared_touched"]
    if not verdict["ok"]:
        out.say(f"allowlist gate FAILED for lane {args.lane.upper()}: {len(verdict['violations'])} violation(s); no speed credit")
        for violation in verdict["violations"][:10]:
            out.say(f"violation: {violation}")
        return

    baselines: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    guard = runner.BoxLock(Path(args.cache_dir).expanduser() / "box.lock", wait_s=min(session.remaining(), 3600.0))
    with _maybe(guard, not (args.no_box_lock or args.offline_fixtures)):
        for pair in range(args.pairs):
            order = ("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")
            for role in order:
                result = session.run_suite(role, baseline_root if role == "baseline" else candidate_root, pair)
                if role == "baseline":
                    baselines.append(session.check_baseline(result, pair))
                    continue
                perf = session.check_candidate(result, pair)
                if perf is None:
                    out.results["pairs_completed"] = len(candidates)
                    return
                candidates.append(perf)
    out.results["pairs_completed"] = len(candidates)
    finish_candidate(args, session, lane, baselines, candidates, verdict)


class _maybe:
    def __init__(self, manager: Any, enabled: bool) -> None:
        self.manager, self.enabled = manager, enabled

    def __enter__(self) -> Any:
        return self.manager.__enter__() if self.enabled else None

    def __exit__(self, *exc: Any) -> None:
        if self.enabled:
            self.manager.__exit__(*exc)


def finish_candidate(args, session, lane, baselines, candidates, verdict) -> None:
    out, manifest = session.out, session.manifest
    scores = scoring.compute(args.lane, manifest, baselines, candidates)
    if scores.errors:
        out.mark("report_gate", False)
        for error in scores.errors:
            out.say(f"scoring error: {error}")
        return
    out.mark("concurrency_gate", not scores.concurrency_failures)
    for failure in scores.concurrency_failures:
        out.say(f"concurrency gate: {failure}")
    results = out.results
    results.update(scores.metrics)
    results["wait_reduction"] = scores.wait_reduction
    results["e2e_improvement_median"] = scores.e2e_improvement_median
    results["e2e_improvement_min"] = scores.e2e_improvement_min
    results["e2e_improvement_max"] = scores.e2e_improvement_max
    results["baseline_targeted_wait_ms"] = scores.baseline_targeted
    results["candidate_targeted_wait_ms"] = scores.candidate_targeted
    results["baseline_e2e_ms_median"] = scores.baseline_e2e
    results["candidate_e2e_ms_median"] = scores.candidate_e2e
    results["makespan_ms"] = scores.candidate_e2e
    results["accepted"] = 1.0 if scores.accepted else 0.0
    valid = not out.failed()
    results["combined_score"] = (
        scoring.combined_score(scores.e2e_improvement_median, scores.wait_reduction, scores.accepted, manifest) if valid else 0.0
    )
    acceptance = manifest["acceptance"]
    verdict_text = "ACCEPTED" if scores.accepted else "valid but NOT accepted" if valid else "INVALID (score 0.0)"
    out.say(f"verdict: {verdict_text}; combined_score={results['combined_score']:.3f}")
    out.say(
        f"lane {args.lane.upper()} ({lane['name']}): targeted wait median {scores.baseline_targeted:.0f} -> "
        f"{scores.candidate_targeted:.0f} ms = {pct(scores.wait_reduction)} reduction (need >= {pct(acceptance['min_wait_reduction'])}); "
        f"makespan median {scores.baseline_e2e:.0f} -> {scores.candidate_e2e:.0f} ms = {pct(scores.e2e_improvement_median)} improvement "
        f"(need >= {pct(acceptance['min_e2e_improvement'])}, pairs {pct(scores.e2e_improvement_min)}..{pct(scores.e2e_improvement_max)})"
    )
    for index, name, base, cand in scores.view_rows:
        out.say(f"per-view MV{index} {name}: lane-targeted {base:.0f} -> {cand:.0f} ms")
    out.say(
        "all wait fields baseline->candidate (ms, medians): "
        + "; ".join(f"{field} {b:.0f}->{c:.0f}" for field, (b, c) in scores.wait_sums.items())
    )
    for warning in verdict["warnings"]:
        out.say(f"warning: {warning}")


def evaluate_baseline(args: argparse.Namespace, session: Session) -> None:
    out, manifest = session.out, session.manifest
    root, _ = prepare_roots(args, session)
    runs: list[dict[str, Any]] = []
    for index in range(args.pairs):
        runs.append(session.check_baseline(session.run_suite("baseline", root, index), index))
    spec = manifest["lanes"][args.lane]["targeted"]
    e2e = [r["makespan_ms"] for r in runs]
    middle = scoring.median(e2e)
    results = out.results
    results["baseline_e2e_ms_median"] = middle
    results["baseline_e2e_spread"] = (max(e2e) - min(e2e)) / middle if middle > 0 else 0.0
    results["baseline_targeted_wait_ms"] = scoring.median([scoring.targeted_value(r, spec) for r in runs])
    results["engine_overlap_ms_baseline_median"] = scoring.median([r["engine_overlap_ms"] for r in runs])
    results["max_concurrent_engine_baseline_median"] = scoring.median([report.engine_concurrency(r) for r in runs])
    results["serial_sum_ms_baseline_median"] = scoring.median([r["serial_sum_ms"] for r in runs])
    results["barrier_hold_overlap_ms_baseline_median"] = scoring.median([r["barrier_hold_overlap_ms"] for r in runs])
    results["pairs_completed"] = len(runs)
    out.say(
        f"baseline mode: {len(runs)} run(s); makespan median {middle:.0f} ms (spread {pct(results['baseline_e2e_spread'])}); "
        "combined_score is always 0.0 in baseline mode"
    )
    out.say("baseline spread above ~15% means this box is too noisy for a 10% acceptance threshold")


def execute(args: argparse.Namespace, manifest: dict[str, Any], run: Any, out: Output) -> None:
    if not args.skip_lock_check:
        problems = lock.verify(lock.ROOT, args.lock_sha256)
        if problems:
            out.mark("trust_gate", False)
            out.say("trust gate FAILED (evaluator files differ from the pinned lock): " + "; ".join(problems[:6]))
            return
    out.mark("trust_gate", True)
    session = Session(args, manifest, run, out)
    if args.mode == "candidate":
        evaluate_candidate(args, session)
    else:
        evaluate_baseline(args, session)
    out.say(out.gate_summary())
    out.say(REMINDER)
    known = manifest["known_gap"]
    if args.lane in known["lanes_that_may_touch"] and args.mode == "candidate":
        out.say(f"note: {known['text']}")


def main(argv: list[str] | None = None, run: Any = None, stream: TextIO | None = None) -> int:
    args = parse_args(argv)
    stream = stream or sys.stdout
    manifest = policy.load_manifest(args.manifest)
    args.pairs = args.pairs if args.pairs is not None else manifest["acceptance"]["default_pairs"]
    args.spark_target = args.spark_target or manifest["nx"]["default_spark_target"]
    args.run_timeout = args.run_timeout or float(manifest["budget"]["run_timeout_s"])
    out = Output(manifest, args.lane)
    code = 0
    try:
        if args.spark_target not in manifest["nx"]["spark_targets"]:
            raise runner.InfraError(f"unknown --spark-target {args.spark_target!r}")
        if run is None:
            if args.offline_fixtures:
                run = runner.FixtureRunner(
                    Path(args.offline_fixtures), manifest, args.offline_baseline_set, args.offline_candidate_set
                )
            else:
                run = runner.NxRunner(manifest)
        execute(args, manifest, run, out)
    except runner.InfraError as exc:
        out.say(f"infra: {exc}")
        code = 2
    except Exception as exc:  # the evaluator must always print a parseable result block
        out.say(f"infra: unexpected {type(exc).__name__}: {exc}")
        code = 2
    out.results["combined_score"] = 0.0 if out.failed() or code else out.results["combined_score"]
    out.emit(stream)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
