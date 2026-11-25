import contextlib
import io
import math
import re
import subprocess
import sys
import unittest

import lock
import runner
import scoring
import state_guard_eval
from . import DARWIN, EVALUATOR, FIXTURES, MANIFEST, scratch_dir

REPORTS = FIXTURES / "reports"
LINE = re.compile(r"^(@RESULT [a-z0-9_]+=-?\d+\.\d{6}|@FEEDBACK \S.*)$")


def parse(text):
    results, feedback = {}, []
    for line in text.splitlines():
        if line.startswith("@RESULT "):
            name, value = line[len("@RESULT "):].split("=", 1)
            results[name] = float(value)
        elif line.startswith("@FEEDBACK "):
            feedback.append(line[len("@FEEDBACK "):])
    return results, feedback


def argv(mode="candidate", lane="a", candidate="good-candidate", pairs=3, *extra):
    return [
        "--mode", mode, "--workspace", str(DARWIN), "--lane", lane, "--pairs", str(pairs),
        "--offline-fixtures", str(REPORTS), "--offline-candidate-set", candidate, *extra,
    ]


def call(arguments, runner_=None):
    stream = io.StringIO()
    with contextlib.redirect_stderr(io.StringIO()):
        code = state_guard_eval.main(arguments, run=runner_, stream=stream)
    results, feedback = parse(stream.getvalue())
    return code, results, feedback


def run_cli(*args, runner_=None, skip_lock=True, **kwargs):
    extra = ["--skip-lock-check"] if skip_lock else []
    return call(argv(*args, **kwargs) + extra, runner_)


class RecordingRunner:
    def __init__(self, override=None):
        self.calls = []
        self.inner = runner.FixtureRunner(REPORTS, MANIFEST, "baseline", "good-candidate")
        self.override = override or {}

    def run(self, role, root, label, spark_target, timeout_s):
        self.calls.append(role)
        if role in self.override:
            return self.override[role]
        return self.inner.run(role, root, label, spark_target, timeout_s)


class AcceptedCandidateTests(unittest.TestCase):
    def test_result_block_has_exactly_the_documented_finite_metrics(self):
        for lane in "abcde":
            with self.subTest(lane=lane):
                code, results, feedback = run_cli(lane=lane)
                self.assertEqual(code, 0)
                self.assertEqual(set(results), set(state_guard_eval.SCALARS) | set(scoring.metric_names(lane, MANIFEST)))
                self.assertTrue(all(math.isfinite(v) for v in results.values()))
                self.assertEqual(results["accepted"], 1.0)
                self.assertEqual(results["pairs_completed"], 3.0)
                self.assertTrue(all(results[g] == 1.0 for g in state_guard_eval.GATES), results)
                self.assertAlmostEqual(
                    results["combined_score"],
                    100 * results["e2e_improvement_median"] + 50 * results["wait_reduction"] + 25,
                    places=2,
                )
                self.assertTrue(any(line.startswith("verdict: ACCEPTED") for line in feedback))

    def test_per_mv_raw_outputs_come_from_the_candidate_median(self):
        _, results, _ = run_cli()
        self.assertEqual(results["makespan_ms"], results["candidate_e2e_ms_median"])
        for index in range(1, 7):
            self.assertIn(f"mv{index}_submit_to_ack_ms", results)
            self.assertIn(f"mv{index}_state_barrier_hold_ms", results)
        self.assertLess(results["candidate_e2e_ms_median"], results["baseline_e2e_ms_median"])

    def test_known_gap_is_echoed_and_shared_file_warning_surfaces(self):
        _, _, feedback = run_cli(lane="b")
        self.assertTrue(any("documented known gap: MV-version/CDF-watermark" in line for line in feedback))
        _, results, feedback = run_cli(lane="a", candidate="lane-a-only")
        self.assertEqual(results["shared_files_touched"], 1.0)
        self.assertTrue(any(line.startswith("warning: shared file") for line in feedback))

    def test_feedback_carries_the_per_view_table_and_the_nested_span_reminder(self):
        _, _, feedback = run_cli()
        self.assertEqual(sum(line.startswith("per-view MV") for line in feedback), 6)
        self.assertTrue(any("nested and non-additive" in line and "NOT proven" in line for line in feedback))
        self.assertTrue(all(len(line) <= state_guard_eval.FEEDBACK_LIMIT for line in feedback))


class InvalidCandidateTests(unittest.TestCase):
    def assert_invalid(self, candidate, gate, needle, lane="a"):
        code, results, feedback = run_cli(lane=lane, candidate=candidate)
        self.assertEqual(code, 0, "invalid candidates are not evaluator errors")
        self.assertEqual(results["combined_score"], 0.0)
        self.assertEqual(results["accepted"], 0.0)
        self.assertEqual(results[gate], 0.0)
        self.assertTrue(any(needle in line for line in feedback), (needle, feedback))
        self.assertTrue(all(math.isfinite(v) for v in results.values()))
        return results

    def test_parity_failure_reports_the_first_failure_lines(self):
        results = self.assert_invalid("bad-parity", "correctness_gate", "MV6 matches the EXCEPT ALL oracle")
        self.assertEqual(results["crash_gate"], 1.0)

    def test_unrecovered_crash_point(self):
        self.assert_invalid("bad-crash", "crash_gate", "crash gate: crash point 'crash-mid-state-publication'")

    def test_full_refresh_regression(self):
        self.assert_invalid("full-refresh-regression", "incremental_gate", "full_refresh_count=1")

    def test_non_finite_report_is_unreadable_not_scored(self):
        self.assert_invalid("nonfinite", "report_gate", "unreadable")

    def test_wrong_evidence_class(self):
        self.assert_invalid("wrong-evidence-class", "report_gate", "evidence_class")

    def test_allowlist_violation_gets_no_speed_credit(self):
        results = self.assert_invalid("bad-allowlist", "allowlist_gate", "violation:")
        self.assertEqual(results["pairs_completed"], 0.0)
        self.assertEqual(results["e2e_improvement_median"], 0.0)

    def test_serialized_and_deferred_candidates_fail_the_concurrency_gate(self):
        for candidate, needle in (("serialized-engines", "serialized"), ("deferred-work", "outside the measured interval")):
            with self.subTest(candidate=candidate):
                results = self.assert_invalid(candidate, "concurrency_gate", needle)
                self.assertGreaterEqual(results["wait_reduction"], 0.8)

    def test_candidate_timeout_is_a_failed_gate(self):
        timed_out = runner.RunResult(exit_code=None, timed_out=True)
        code, results, feedback = run_cli(runner_=RecordingRunner({"candidate": timed_out}))
        self.assertEqual((code, results["combined_score"], results["correctness_gate"]), (0, 0.0, 0.0))
        self.assertTrue(any("timed out" in line for line in feedback))

    def test_secrets_in_suite_output_never_reach_stdout(self):
        failing = runner.RunResult(exit_code=1, failure_lines=["*** FAILED *** token=abc123 ghp_" + "A" * 30])
        _, results, feedback = run_cli(runner_=RecordingRunner({"candidate": failing}))
        self.assertEqual(results["combined_score"], 0.0)
        text = "\n".join(feedback)
        self.assertIn("*** FAILED ***", text)
        self.assertNotIn("abc123", text)
        self.assertNotIn("ghp_A", text)


class ValidButNotAcceptedTests(unittest.TestCase):
    def test_slower_valid_candidate_scores_above_the_invalid_zero(self):
        _, results, _ = run_cli(candidate="slow-valid")
        self.assertEqual(results["accepted"], 0.0)
        self.assertEqual(results["combined_score"], MANIFEST["scoring"]["valid_floor"])
        self.assertLess(results["e2e_improvement_median"], 0)

    def test_a_lane_a_change_earns_no_wait_credit_in_other_lanes(self):
        _, results, _ = run_cli(lane="c", candidate="lane-a-only")
        self.assertEqual(results["accepted"], 0.0)
        self.assertEqual(results["wait_reduction"], 0.0)
        self.assertGreater(results["combined_score"], 0.0)


class PairingTests(unittest.TestCase):
    def test_runs_alternate_baseline_and_candidate_first(self):
        recorder = RecordingRunner()
        run_cli(runner_=recorder, pairs=3)
        self.assertEqual(
            recorder.calls, ["baseline", "candidate", "candidate", "baseline", "baseline", "candidate"]
        )

    def test_fewer_than_three_pairs_is_an_infrastructure_error(self):
        code, results, feedback = run_cli(pairs=2)
        self.assertEqual((code, results["combined_score"]), (2, 0.0))
        self.assertTrue(any(line.startswith("infra:") and "--pairs" in line for line in feedback))

    def test_unknown_fixture_set_is_an_infrastructure_error_with_a_zero_score(self):
        code, results, _ = run_cli(candidate="does-not-exist")
        self.assertEqual((code, results["combined_score"]), (2, 0.0))


class BaselineModeTests(unittest.TestCase):
    def test_baseline_mode_reports_spread_and_never_scores(self):
        code, results, feedback = run_cli(mode="baseline", pairs=3)
        self.assertEqual(code, 0)
        self.assertEqual(results["combined_score"], 0.0)
        self.assertEqual(results["baseline_e2e_ms_median"], 48698.0)
        self.assertEqual(results["baseline_e2e_spread"], 0.0)
        self.assertEqual(results["max_concurrent_engine_baseline_median"], 6.0)
        self.assertTrue(any("baseline mode" in line for line in feedback))

    def test_an_unhealthy_baseline_is_an_infrastructure_error(self):
        code, results, feedback = call(
            argv(mode="baseline", pairs=3) + ["--offline-baseline-set", "bad-parity", "--skip-lock-check"]
        )
        self.assertEqual((code, results["combined_score"]), (2, 0.0))
        self.assertTrue(any("baseline run 1 failed" in line for line in feedback))


class TrustGateTests(unittest.TestCase):
    def with_lock(self, digest):
        return call(argv() + ["--lock-sha256", digest])

    def test_pinned_lock_digest_passes_and_a_wrong_one_scores_zero(self):
        code, results, _ = self.with_lock(lock.lock_digest(lock.ROOT))
        self.assertEqual((code, results["trust_gate"]), (0, 1.0))
        self.assertEqual(results["accepted"], 1.0)
        code, results, feedback = self.with_lock("0" * 64)
        self.assertEqual((code, results["trust_gate"], results["combined_score"]), (0, 0.0, 0.0))
        self.assertTrue(any("trust gate FAILED" in line for line in feedback))

    def test_modified_evaluator_file_fails_the_trust_gate(self):
        with scratch_dir() as tmp:
            copy = tmp / "darwin"
            (copy / "evaluator").mkdir(parents=True)
            for rel in lock.tracked(lock.ROOT) + [lock.LOCK_NAME]:
                (copy / rel).write_bytes((lock.ROOT / rel).read_bytes())
            self.assertEqual(lock.verify(copy), [])
            with open(copy / "evaluator" / "scoring.py", "a", encoding="utf-8") as handle:
                handle.write("\nCLAMP = 1e308\n")
            self.assertEqual(lock.verify(copy), ["evaluator/scoring.py: digest mismatch"])


class SubprocessTests(unittest.TestCase):
    def test_stdout_is_only_protocol_lines_and_exit_code_is_zero(self):
        completed = subprocess.run(
            [sys.executable, str(EVALUATOR / "state_guard_eval.py"), *argv(), "--skip-lock-check"],
            capture_output=True,
            text=True,
            timeout=120,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        lines = completed.stdout.splitlines()
        self.assertTrue(lines)
        for line in lines:
            self.assertRegex(line, LINE)
        self.assertIn("pair 1/3 baseline run starting", completed.stderr)


if __name__ == "__main__":
    unittest.main()
