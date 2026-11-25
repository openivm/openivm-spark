import os
import subprocess
import sys
import time
import unittest
from unittest import mock

import policy
import runner
from . import FIXTURES, MANIFEST, scratch_dir

REPORT_ENV = MANIFEST["nx"]["report_dir_env"]
GIT = ["git", "-c", "user.name=t", "-c", "user.email=t@example.invalid", "-c", "commit.gpgsign=false"]
GOOD = FIXTURES / "reports" / "good-candidate"
FAKE_SUITE = """
import os, shutil, sys
mode = sys.argv[1]
out = os.environ["{env}"]
print("TOKENS_LEAKED" if any("TOKEN" in key for key in os.environ) else "env clean")
print("clone https://user:hunter2@example.invalid/repo.git with ghp_" + "B" * 30)
if mode == "pass":
    for name in ("state-guard-perf.json", "state-guard-crash.json"):
        shutil.copy(r"{good}" + os.sep + name, os.path.join(out, name))
elif mode == "fail":
    print("[info] - MV3 matches the EXCEPT ALL oracle *** FAILED ***")
    sys.exit(1)
elif mode == "hang":
    import time
    time.sleep(120)
""".format(env=REPORT_ENV, good=str(GOOD))


class FakeNx(runner.NxRunner):
    def __init__(self, mode):
        super().__init__(MANIFEST, exec_prefix="")
        self.mode = mode

    def command(self, spark_target, report_dir, label):
        return [sys.executable, "-c", FAKE_SUITE, self.mode]


class RedactionTests(unittest.TestCase):
    def test_token_shapes_are_redacted(self):
        cases = (
            ("token ghp_" + "A" * 36 + " used", "ghp_" + "A" * 36),
            ("gho_" + "Z9" * 12, "gho_" + "Z9" * 12),
            ("pat github_pat_" + "x" * 30, "github_pat_" + "x" * 30),
            ("Authorization: Bearer abc.def-ghi", "abc.def-ghi"),
            ("fetch https://bot:hunter2@github.com/o/r.git", "hunter2"),
            ("GITHUB_TOKEN=s3cr3t", "s3cr3t"),
            ("db_password: p4ss", "p4ss"),
        )
        for line, secret in cases:
            with self.subTest(secret=secret[:6]):
                cleaned = runner.redact(line)
                self.assertNotIn(secret, cleaned)
                self.assertIn("REDACTED", cleaned)

    def test_ordinary_log_lines_are_untouched(self):
        line = "[info] - MV6 matches the EXCEPT ALL oracle (StateGuardPerfSpec.scala:212) run_id=12345"
        self.assertEqual(runner.redact(line), line)

    def test_secret_environment_is_not_inherited_by_the_suite(self):
        with mock.patch.dict(os.environ, {"GH_TOKEN": "x", "MY_API_KEY": "y", "AZURE_CLIENT_SECRET": "z", "HARMLESS": "ok"}):
            env = runner.scrubbed_env()
        self.assertEqual(set(env) & {"GH_TOKEN", "MY_API_KEY", "AZURE_CLIENT_SECRET"}, set())
        self.assertEqual(env["HARMLESS"], "ok")


class NxCommandTests(unittest.TestCase):
    def test_host_command_uses_the_locked_nx_cli(self):
        command = runner.NxRunner(MANIFEST, exec_prefix="").command("spark-4.1", "/r", "lbl")
        self.assertEqual(
            command,
            ["npx", "--no-install", "nx", "run", "spark-ext:state-guard-perf", "--configuration=spark-4.1"],
        )

    def test_devcontainer_command_exports_the_report_contract(self):
        command = runner.NxRunner(MANIFEST, exec_prefix="npx --no-install nx run devcontainer:exec --").command(
            "spark-3.5", "/workspaces/openivm-spark/.logs/state-guard/x", "x"
        )
        self.assertEqual(command[:7], ["npx", "--no-install", "nx", "run", "devcontainer:exec", "--", "env"])
        self.assertIn(f"{REPORT_ENV}=/workspaces/openivm-spark/.logs/state-guard/x", command)
        self.assertIn("OPENIVM_STATE_GUARD_RUN_LABEL=x", command)
        self.assertEqual(command[-1], "--configuration=spark-3.5")


class NxRunnerProcessTests(unittest.TestCase):
    def run_fake(self, mode, timeout=60):
        with scratch_dir() as root:
            result = FakeNx(mode).run("candidate", root, "case", "spark-3.5", timeout)
            log = (root / ".logs" / "state-guard" / "case.run.log")
            return result, log.read_text(encoding="utf-8") if log.is_file() else ""

    def test_reports_are_collected_and_the_log_is_redacted(self):
        result, log = self.run_fake("pass")
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.read_errors, [])
        self.assertEqual(result.perf["schema"], "openivm.state-guard-perf/v1")
        self.assertEqual(result.crash["schema"], "openivm.state-guard-crash/v1")
        self.assertIn("env clean", log)
        self.assertNotIn("hunter2", log)
        self.assertNotIn("ghp_B", log)

    def test_failing_suite_keeps_the_failure_lines_and_reports_missing_files(self):
        result, _ = self.run_fake("fail")
        self.assertEqual(result.exit_code, 1)
        self.assertIsNone(result.perf)
        self.assertIn("state-guard-perf.json: not written", result.read_errors)
        self.assertEqual(result.failure_lines, ["[info] - MV3 matches the EXCEPT ALL oracle *** FAILED ***"])
        self.assertTrue(all("hunter2" not in line for line in result.failure_excerpt()))

    def test_hung_suite_is_killed_at_the_timeout(self):
        started = time.monotonic()
        result, _ = self.run_fake("hang", timeout=2)
        self.assertTrue(result.timed_out)
        self.assertIsNone(result.exit_code)
        self.assertLess(time.monotonic() - started, 60)


class FixtureRunnerTests(unittest.TestCase):
    def test_exit_code_and_failure_lines_come_from_run_json(self):
        fixtures = runner.FixtureRunner(FIXTURES / "reports", MANIFEST, "baseline", "bad-parity")
        result = fixtures.run("candidate", FIXTURES, "x", "spark-3.5", 1.0)
        self.assertEqual(result.exit_code, 1)
        self.assertTrue(any("*** FAILED ***" in line for line in result.failure_lines))
        self.assertIsNotNone(result.perf)
        self.assertEqual(fixtures.run("baseline", FIXTURES, "x", "spark-3.5", 1.0).exit_code, 0)

    def test_missing_set_is_an_infrastructure_error(self):
        with self.assertRaises(runner.InfraError):
            runner.FixtureRunner(FIXTURES / "reports", MANIFEST, "baseline", "nope").run("candidate", FIXTURES, "x", "t", 1.0)


class BoxLockTests(unittest.TestCase):
    def test_lock_is_exclusive_and_released(self):
        with scratch_dir() as tmp:
            path = tmp / "box.lock"
            with runner.BoxLock(path, wait_s=0, sleep=lambda _: None):
                self.assertTrue(path.exists())
                with self.assertRaises(runner.InfraError):
                    with runner.BoxLock(path, wait_s=0, sleep=lambda _: None):
                        pass
                self.assertTrue(path.exists(), "a refused contender must not release the holder's lock")
            self.assertFalse(path.exists())

    def test_stale_lock_from_a_crashed_holder_is_stolen(self):
        with scratch_dir() as tmp:
            path = tmp / "box.lock"
            path.write_text("pid=1\n")
            os.utime(path, (time.time() - 100, time.time() - 100))
            with runner.BoxLock(path, wait_s=0, stale_s=10, sleep=lambda _: None):
                self.assertTrue(path.exists())


class BaselineWorktreeTests(unittest.TestCase):
    def setUp(self):
        context = scratch_dir()
        self.tmp = context.__enter__()
        self.addCleanup(context.__exit__, None, None, None)
        self.source = self.tmp / "source"
        self.source.mkdir()
        self.git(self.source, "init", "--quiet")
        (self.source / "f.txt").write_text("one\n")
        self.git(self.source, "add", "-A")
        self.git(self.source, "commit", "--quiet", "-m", "one")
        self.first = self.git(self.source, "rev-parse", "HEAD")
        (self.source / "f.txt").write_text("two\n")
        self.git(self.source, "commit", "--quiet", "-am", "two")
        self.second = self.git(self.source, "rev-parse", "HEAD")

    @staticmethod
    def git(root, *args):
        return subprocess.run([*GIT, *args], cwd=root, check=True, capture_output=True, text=True).stdout.strip()

    def test_baseline_checkout_is_pinned_and_reset_between_pairs(self):
        cache = self.tmp / "cache"
        path = runner.prepare_baseline_worktree(cache, self.first, [str(self.source)])
        self.assertEqual((path / "f.txt").read_text(), "one\n")
        (path / "f.txt").write_text("dirty\n")
        (path / "stray.txt").write_text("x")
        again = runner.prepare_baseline_worktree(cache, self.first, [str(self.source)])
        self.assertEqual(again, path)
        self.assertEqual((path / "f.txt").read_text(), "one\n")
        self.assertFalse((path / "stray.txt").exists())
        self.assertEqual(self.git(path, "rev-parse", "HEAD"), self.first)

    def test_unfetchable_baseline_is_an_infrastructure_error(self):
        with self.assertRaises(runner.InfraError):
            runner.prepare_baseline_worktree(self.tmp / "cache", "0" * 40, [str(self.source)])

    def test_resolve_baseline_prefers_the_local_object_then_fetches(self):
        self.assertEqual(policy.resolve_baseline(self.source, "HEAD~1"), self.first)
        clone = self.tmp / "clone"
        self.git(self.tmp, "clone", "--quiet", "--depth=1", self.source.as_uri(), str(clone))
        self.assertEqual(policy.resolve_baseline(clone, self.second), self.second)
        with self.assertRaises(policy.PolicyError):
            policy.resolve_baseline(clone, "refs/heads/does-not-exist")


if __name__ == "__main__":
    unittest.main()
