import asyncio
import sys
import unittest
from unittest import mock

import darwin_evaluator as wrapper
from . import EVALUATOR, FIXTURES

REPORTS = FIXTURES / "reports"


def options(**overrides):
    values = {"lane": "a", "baseline_ref": "abc123", "pairs": 3, "deadline_s": 5.0}
    values.update(overrides)
    return wrapper.StateGuardOptions(**values)


def evaluator(**overrides):
    instance = wrapper.StateGuardEvaluator.__new__(wrapper.StateGuardEvaluator)
    object.__setattr__(instance, "options", options(**overrides))
    return instance


def offline_argv(candidate):
    return [
        sys.executable, str(EVALUATOR / "state_guard_eval.py"), "--mode", "candidate", "--workspace", str(EVALUATOR),
        "--lane", "a", "--pairs", "3", "--offline-fixtures", str(REPORTS), "--offline-candidate-set", candidate,
        "--skip-lock-check",
    ]


class ParseOutputTests(unittest.TestCase):
    def test_results_and_feedback_are_split(self):
        metrics, feedback = wrapper.parse_output(
            "@FEEDBACK verdict: ACCEPTED\n@RESULT combined_score=12.500000\n@RESULT correctness_gate=1.000000\nnoise\n"
        )
        self.assertEqual(metrics, {"combined_score": 12.5, "correctness_gate": 1.0})
        self.assertEqual(feedback, ["verdict: ACCEPTED"])

    def test_non_finite_and_absurd_values_become_sane_floats(self):
        metrics, _ = wrapper.parse_output(
            "@RESULT a=nan\n@RESULT b=inf\n@RESULT c=1e301\n@RESULT d=-1e301\n@RESULT e=garbage\n@RESULT combined_score=1\n"
        )
        self.assertEqual(metrics["a"], 0.0)
        self.assertEqual(metrics["b"], 0.0)
        self.assertEqual(metrics["c"], 1e12)
        self.assertEqual(metrics["d"], -1e12)
        self.assertEqual(metrics["e"], 0.0)

    def test_any_failed_gate_zeroes_the_score_even_if_the_cli_reported_one(self):
        metrics, _ = wrapper.parse_output("@RESULT combined_score=99\n@RESULT crash_gate=0.000000\n@RESULT allowlist_gate=1\n")
        self.assertEqual(metrics["combined_score"], 0.0)

    def test_missing_combined_score_defaults_to_zero(self):
        self.assertEqual(wrapper.parse_output("@FEEDBACK x")[0], {"combined_score": 0.0})

    def test_feedback_is_redacted(self):
        _, feedback = wrapper.parse_output("@FEEDBACK clone failed: ghp_" + "C" * 30 + " Bearer abc\n")
        self.assertNotIn("ghp_C", feedback[0])
        self.assertNotIn("abc", feedback[0])


class ResultFromCliTests(unittest.TestCase):
    def test_normal_run(self):
        result = wrapper.result_from_cli(0, "@FEEDBACK ok\n@RESULT combined_score=7.000000\n", "progress")
        self.assertEqual(result.metrics["combined_score"], 7.0)
        self.assertEqual(result.metrics["infra_error"], 0.0)
        self.assertEqual(result.artifacts, {"feedback": "ok"})

    def test_infrastructure_exit_code_zeroes_the_score_and_keeps_the_log(self):
        result = wrapper.result_from_cli(2, "@FEEDBACK infra: boom\n@RESULT combined_score=5.000000\n", "trace line")
        self.assertEqual((result.metrics["combined_score"], result.metrics["infra_error"]), (0.0, 1.0))
        self.assertIn("infra: boom", result.artifacts["feedback"])
        self.assertEqual(result.artifacts["evaluator_log"], "trace line")

    def test_crash_timeout_and_garbage_never_raise_and_score_zero(self):
        for exit_code, stdout in ((None, ""), (1, "Traceback ..."), (0, "no protocol lines")):
            with self.subTest(exit_code=exit_code):
                result = wrapper.result_from_cli(exit_code, stdout, "stderr text")
                self.assertEqual(result.metrics["combined_score"], 0.0)
                self.assertEqual(result.metrics["infra_error"], 1.0)
                self.assertIn("combined_score 0.0", result.artifacts["feedback"])


class BuildArgvTests(unittest.TestCase):
    def test_required_flags_and_optional_flags(self):
        argv = wrapper.build_argv(options(), "/work")
        self.assertEqual(argv[2:6], ["--mode", "candidate", "--workspace", "/work"])
        self.assertEqual(argv[argv.index("--lane") + 1], "a")
        self.assertEqual(argv[argv.index("--baseline-ref") + 1], "abc123")
        self.assertNotIn("--lock-sha256", argv)
        self.assertNotIn("--baseline-repo", argv)
        pinned = wrapper.build_argv(options(lock_sha256="f" * 64, baseline_repo="https://example.invalid/r.git"), "/work")
        self.assertEqual(pinned[pinned.index("--lock-sha256") + 1], "f" * 64)
        self.assertEqual(pinned[pinned.index("--baseline-repo") + 1], "https://example.invalid/r.git")


class RunCliTests(unittest.IsolatedAsyncioTestCase):
    async def test_exit_code_and_streams(self):
        code, out, err = await wrapper._run_cli([sys.executable, "-c", "import sys; print('o'); print('e', file=sys.stderr); sys.exit(3)"], 30)
        self.assertEqual((code, out.strip(), err.strip()), (3, "o", "e"))

    async def test_timeout_kills_the_process_and_returns_none(self):
        code, out, _ = await wrapper._run_cli([sys.executable, "-c", "import time; time.sleep(60)"], 1)
        self.assertEqual((code, out), (None, ""))

    async def test_real_cli_output_round_trips_through_the_wrapper(self):
        code, out, err = await wrapper._run_cli(offline_argv("good-candidate"), 120)
        accepted = wrapper.result_from_cli(code, out, err)
        self.assertEqual((accepted.metrics["accepted"], accepted.metrics["infra_error"]), (1.0, 0.0))
        self.assertGreater(accepted.metrics["combined_score"], 100)
        code, out, err = await wrapper._run_cli(offline_argv("bad-parity"), 120)
        rejected = wrapper.result_from_cli(code, out, err)
        self.assertEqual((rejected.metrics["combined_score"], rejected.metrics["correctness_gate"]), (0.0, 0.0))
        self.assertIn("EXCEPT ALL oracle", rejected.artifacts["feedback"])
        self.assertTrue(all(isinstance(v, float) for v in rejected.metrics.values()))


class EvaluateTests(unittest.IsolatedAsyncioTestCase):
    async def test_success_path_uses_the_candidate_workspace(self):
        seen = {}

        async def fake_run(argv, timeout):
            seen["argv"], seen["timeout"] = argv, timeout
            return 0, "@FEEDBACK fine\n@RESULT combined_score=3.000000\n", ""

        subject = evaluator(deadline_s=10.0)
        with mock.patch.object(subject, "_workspace", mock.AsyncMock(return_value="/cand")), mock.patch.object(wrapper, "_run_cli", fake_run):
            result = await subject.evaluate(mock.Mock())
        self.assertEqual(result.metrics["combined_score"], 3.0)
        self.assertIn("/cand", seen["argv"])
        self.assertEqual(seen["timeout"], 10.0 + wrapper.GRACE_S)

    async def test_wrapper_errors_become_a_zero_score_not_an_exception(self):
        subject = evaluator()
        with mock.patch.object(subject, "_workspace", mock.AsyncMock(side_effect=RuntimeError("no workspace"))):
            result = await subject.evaluate(mock.Mock())
        self.assertEqual((result.metrics["combined_score"], result.metrics["infra_error"]), (0.0, 1.0))
        self.assertIn("no workspace", result.artifacts["feedback"])


class ModuleContractTests(unittest.TestCase):
    def test_exactly_one_concrete_evaluator_subclass_is_defined(self):
        import inspect

        concrete = [
            obj for obj in vars(wrapper).values()
            if inspect.isclass(obj) and issubclass(obj, wrapper.BaseEvaluator) and obj is not wrapper.BaseEvaluator and not inspect.isabstract(obj)
        ]
        self.assertEqual(concrete, [wrapper.StateGuardEvaluator])
        self.assertNotIn("__init__", vars(wrapper.StateGuardEvaluator))


if __name__ == "__main__":
    unittest.main()
