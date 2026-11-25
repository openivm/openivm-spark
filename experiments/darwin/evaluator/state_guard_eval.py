#!/usr/bin/env python3
"""Trusted state-guard evaluator CLI.

Scores a candidate workspace against a fresh same-box baseline. It runs the crash-recovery suite
first (a failed gate means no timed pair is run or credited), then interleaved pairs of the perf
suite, both from the trusted `state-guard-perf` Nx target. Prints `@FEEDBACK <text>` and
`@RESULT <name>=<float>` lines on stdout; progress goes to stderr. Exit 0 for any candidate
outcome (invalid => score 0.0), 2 for evaluator infrastructure errors.
"""

import argparse
import json
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
    "recovery_qualified",
    "crash_gated_points",
    "crash_failing_points",
    "crash_baseline_failing_points",
    "crash_fixed_points",
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
    parser.add_argument("--run-timeout", type=float, default=None, help="seconds per perf suite run")
    parser.add_argument("--crash-timeout", type=float, default=None, help="seconds per crash suite run")
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
        self.baseline_sha = ""
        self.baseline_verdict: report.CrashVerdict | None = None
        self.candidate_verdict: report.CrashVerdict | None = None

    def remaining(self) -> float:
        if self.args.deadline <= 0:
            return float("inf")
        return self.args.deadline - (time.monotonic() - self.started)

    def run_suite(self, role: str, root: Path, pair: int, suite: str = "perf") -> runner.RunResult:
        limit = self.args.crash_timeout if suite == "crash" else self.args.run_timeout
        timeout = min(limit, self.remaining())
        if timeout <= 0:
            return runner.RunResult(exit_code=None, timed_out=True)
        self.run_count += 1
        label = f"{role}-{suite}-{os.getpid()}-{pair}"
        what = f"pair {pair + 1}/{self.args.pairs} {role} perf" if suite == "perf" else f"{role} crash"
        log(f"{what} run starting (timeout {timeout:.0f}s)")
        result = self.runner.run(role, root, label, self.args.spark_target, timeout, suite)
        log(f"{what} run finished: exit={result.exit_code} timed_out={result.timed_out} {result.duration_s:.0f}s")
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
        perf_name = manifest["nx"]["perf_report"]
        if result.timed_out:
            out.mark("correctness_gate", False)
            out.say(f"candidate run {pair + 1} timed out or the evaluation deadline was reached")
            return None
        if result.exit_code != 0:
            out.mark("correctness_gate", False)
            out.say(f"perf suite exited {result.exit_code} (a correctness assertion or the build failed)")
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

    def crash_verdict(self, result: runner.RunResult) -> tuple[report.CrashVerdict | None, list[str]]:
        """The validated crash verdict of one crash-suite run, or the reasons it cannot be used."""
        name = self.manifest["nx"]["crash_report"]
        excerpt = [f"failure: {line}" for line in result.failure_excerpt(6)]
        if result.timed_out:
            return None, ["the crash suite timed out or the evaluation deadline was reached"]
        if result.crash is None:
            reason = next((e for e in result.read_errors if e.startswith(name)), f"{name}: not written")
            return None, [f"{reason} (the build failed or the suite aborted before reporting)", *excerpt]
        problems = report.validate_crash(result.crash, self.manifest)
        if problems:
            return None, [f"invalid {name}: " + "; ".join(problems[:6])]
        verdict = report.crash_verdict(result.crash, self.manifest)
        if verdict.problems:
            return None, [f"invalid {name}: " + "; ".join(verdict.problems[:6])]
        if result.exit_code != 0 and not verdict.failing:
            return None, [f"the crash suite exited {result.exit_code} although its report shows no failing point", *excerpt]
        return verdict, []

    def cached_baseline_crash(self) -> Path | None:
        if self.args.offline_fixtures or not self.baseline_sha:
            return None
        backend = os.environ.get("OPENIVM_STATE_GUARD_CATALOG_BACKEND", "delta") or "delta"
        name = f"{self.baseline_sha[:12]}-{self.args.spark_target}-{backend}.json"
        return Path(self.args.cache_dir).expanduser() / "crash-baseline" / name

    def baseline_crash(self, root: Path) -> report.CrashVerdict:
        """Crash verdict of the same-box baseline; computed once per baseline/target/backend and cached."""
        cache = self.cached_baseline_crash()
        verdict = None
        if cache is not None and cache.is_file():
            try:
                cached = report.load_json(cache)
                if not report.validate_crash(cached, self.manifest):
                    verdict = report.crash_verdict(cached, self.manifest)
                    log(f"baseline crash report reused from {cache}")
            except (OSError, ValueError):
                verdict = None
            if verdict is not None and verdict.problems:
                verdict = None
        if verdict is None:
            result = self.run_suite("baseline", root, 0, "crash")
            verdict, reasons = self.crash_verdict(result)
            if verdict is None:
                raise runner.InfraError("baseline crash suite is unusable: " + "; ".join(reasons[:4]))
            if cache is not None:
                cache.parent.mkdir(parents=True, exist_ok=True)
                cache.write_text(json.dumps(result.crash, indent=2, sort_keys=True), encoding="utf-8")
        self.baseline_verdict = verdict
        return verdict

    def check_crash(self, candidate_root: Path, baseline_root: Path) -> bool:
        """Run the candidate crash suite; False (gate failed) means no speed is measured or awarded."""
        out = self.out
        baseline = self.baseline_crash(baseline_root)
        known_failures = baseline.failing
        result = self.run_suite("candidate", candidate_root, 0, "crash")
        verdict, reasons = self.crash_verdict(result)
        if verdict is None:
            out.mark("crash_gate", False)
            for reason in reasons:
                out.say(f"crash gate: {reason}")
            return False
        self.candidate_verdict = verdict
        regressions = report.crash_regressions(verdict, baseline)
        out.mark("crash_gate", not regressions)
        for regression in regressions[:6]:
            out.say(f"crash gate: {regression}")
        results = out.results
        results["crash_gated_points"] = verdict.gated
        results["crash_failing_points"] = len(verdict.failing)
        results["crash_baseline_failing_points"] = len(known_failures)
        results["crash_fixed_points"] = len([key for key in known_failures if key not in verdict.failing])
        results["recovery_qualified"] = 1.0 if not verdict.failing and not regressions else 0.0
        self.explain_crash(verdict, known_failures)
        return not regressions

    def explain_crash(self, verdict: report.CrashVerdict, known_failures: dict[str, list[str]]) -> None:
        out = self.out
        if verdict.failing:
            listed = "; ".join(
                f"{key} ({', '.join(flags)} false{', baseline fails too' if key in known_failures else ', NEW'})"
                for key, flags in verdict.failing.items()
            )
            out.say(
                "recovery NOT QUALIFIED: these gated crash points still fail (baseline-known failures stay visible and are "
                f"never scored as success; speed is exploratory and cannot be accepted until they pass): {listed}"
            )
        else:
            out.say(f"recovery qualified: all {verdict.gated} gated crash points pass (intercept and cdf reported separately)")
        fixed = [key for key in known_failures if key not in verdict.failing]
        if fixed:
            out.say("crash points fixed versus the same-box baseline: " + ", ".join(fixed))
        for key in verdict.known:
            out.say(f"{self.manifest['known_gap']['text']} [point {key}] reported separately; never counted as a pass")


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
    session.baseline_sha = sha
    changes = policy.collect_changes(args.workspace, sha) if args.mode == "candidate" else []
    if args.mode == "candidate":
        verdict = policy.check_changes(changes, manifest, args.lane)
        if not verdict["ok"]:
            return Path(args.workspace), changes
    protect = (Path(args.workspace), lock.ROOT)
    root = runner.prepare_baseline_worktree(Path(args.cache_dir).expanduser(), sha, baseline_sources(args), protect)
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
        if not session.check_crash(candidate_root, baseline_root):
            return
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
    qualified = results["recovery_qualified"] == 1.0
    accepted = scores.accepted and qualified
    results["accepted"] = 1.0 if accepted else 0.0
    valid = not out.failed()
    results["combined_score"] = (
        scoring.combined_score(scores.e2e_improvement_median, scores.wait_reduction, accepted, manifest) if valid else 0.0
    )
    acceptance = manifest["acceptance"]
    if not valid:
        verdict_text = "INVALID (score 0.0)"
    elif accepted:
        verdict_text = "ACCEPTED"
    elif scores.accepted:
        verdict_text = "speed thresholds met but NOT accepted: recovery is not qualified"
    else:
        verdict_text = "valid but NOT accepted"
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
    known_failures = session.baseline_crash(root).failing
    runs: list[dict[str, Any]] = []
    for index in range(args.pairs):
        runs.append(session.check_baseline(session.run_suite("baseline", root, index), index))
    spec = manifest["lanes"][args.lane]["targeted"]
    e2e = [r["makespan_ms"] for r in runs]
    middle = scoring.median(e2e)
    results = out.results
    results["crash_gated_points"] = session.baseline_verdict.gated
    results["crash_baseline_failing_points"] = len(known_failures)
    results["crash_failing_points"] = len(known_failures)
    results["recovery_qualified"] = 1.0 if not known_failures else 0.0
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
    if known_failures:
        listed = "; ".join(f"{key} ({', '.join(flags)} false)" for key, flags in known_failures.items())
        out.say(f"baseline recovery failures are real product failures, never filtered or scored as success: {listed}")
    else:
        out.say(f"baseline recovery: all {session.baseline_verdict.gated} gated crash points pass")
    for key in session.baseline_verdict.known:
        out.say(f"{manifest['known_gap']['text']} [point {key}] reported separately; never counted as a pass")


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
    args.crash_timeout = args.crash_timeout or float(manifest["budget"]["crash_timeout_s"])
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
