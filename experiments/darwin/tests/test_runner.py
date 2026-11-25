import os
import subprocess
import sys
import time
import unittest
from pathlib import Path
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
elif mode == "chdir":
    os.chdir(os.path.dirname(out))
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

    def command(self, spark_target, report_dir, label, suite="all"):
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
            "spark-3.5", "/workspaces/openivm-spark/.logs/state-guard/x", "x", "crash"
        )
        self.assertEqual(command[:7], ["npx", "--no-install", "nx", "run", "devcontainer:exec", "--", "env"])
        self.assertIn(f"{REPORT_ENV}=/workspaces/openivm-spark/.logs/state-guard/x", command)
        self.assertIn("OPENIVM_STATE_GUARD_RUN_LABEL=x", command)
        self.assertIn(f"{MANIFEST['nx']['suite_env']}=crash", command)
        self.assertEqual(command[-1], "--configuration=spark-3.5")

    def test_unknown_suite_is_an_infrastructure_error(self):
        with scratch_dir() as root, self.assertRaises(runner.InfraError):
            FakeNx("pass").run("candidate", root, "x", "spark-3.5", 5, "everything")


class FreshWorktreeDependencyTests(unittest.TestCase):
    def test_installed_or_prefixed_roots_are_left_alone(self):
        with scratch_dir() as root, mock.patch.object(runner.subprocess, "run") as npm:
            runner.ensure_dependencies(root, ["npx"])
            (root / "node_modules" / ".bin").mkdir(parents=True)
            (root / "node_modules" / ".bin" / "nx").write_text("")
            runner.ensure_dependencies(root)
        npm.assert_not_called()

    def test_missing_node_modules_install_from_the_frozen_lockfile(self):
        with scratch_dir() as root:
            (root / "package-lock.json").write_text("{}")

            def fake_npm(command, **kwargs):
                (root / "node_modules" / ".bin").mkdir(parents=True)
                (root / "node_modules" / ".bin" / "nx").write_text("")
                return subprocess.CompletedProcess(command, 0, stdout="")

            with mock.patch.object(runner.subprocess, "run", side_effect=fake_npm) as npm:
                runner.ensure_dependencies(root)
            self.assertEqual(npm.call_args.args[0][:2], ["npm", "ci"])
            self.assertEqual(npm.call_args.kwargs["cwd"], str(root))

    def test_no_lockfile_or_a_failed_install_is_an_infrastructure_error(self):
        with scratch_dir() as root:
            with self.assertRaises(runner.InfraError):
                runner.ensure_dependencies(root)
            (root / "package-lock.json").write_text("{}")
            failed = subprocess.CompletedProcess([], 1, stdout="ERESOLVE could not resolve")
            with mock.patch.object(runner.subprocess, "run", return_value=failed), self.assertRaises(runner.InfraError):
                runner.ensure_dependencies(root)


class NxRunnerProcessTests(unittest.TestCase):
    def run_fake(self, mode, timeout=60):
        with scratch_dir() as root:
            (root / "node_modules" / ".bin").mkdir(parents=True)
            (root / "node_modules" / ".bin" / "nx").touch()
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

    def test_a_relative_workspace_still_gives_the_suite_an_absolute_report_dir(self):
        with scratch_dir() as root:
            (root / "node_modules" / ".bin").mkdir(parents=True)
            (root / "node_modules" / ".bin" / "nx").touch()
            previous = os.getcwd()
            os.chdir(root)
            try:
                result = FakeNx("chdir").run("candidate", Path("."), "case", "spark-3.5", 60)
            finally:
                os.chdir(previous)
        self.assertEqual(result.read_errors, [])
        self.assertEqual(result.crash["schema"], "openivm.state-guard-crash/v1")

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


class JunitOutcomeTests(unittest.TestCase):
    SUITE = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<testsuite name="{cls}" tests="3">\n'
        '<testcase classname="{cls}" name="same name" time="0.1"/>\n'
        '<testcase classname="{cls}" name="broken" time="0.1"><failure message="x" type="t">trace</failure></testcase>\n'
        '<testcase classname="{cls}" name="ignored" time="0.0"><skipped/></testcase>\n'
        "</testsuite>\n"
    )

    def write(self, directory, name, text):
        (directory / name).write_text(text, encoding="utf-8")

    def test_outcomes_are_keyed_by_simple_suite_name(self):
        with scratch_dir() as root:
            self.write(root, "TEST-a.One.xml", self.SUITE.format(cls="a.One"))
            self.write(root, "TEST-b.Two.xml", self.SUITE.replace('<failure message="x" type="t">trace</failure>', "").format(cls="b.Two"))
            outcomes = runner.read_junit_outcomes(root)
        self.assertEqual(
            outcomes,
            {
                "One: same name": "passed",
                "One: broken": "failed",
                "One: ignored": "skipped",
                "Two: same name": "passed",
                "Two: broken": "passed",
                "Two: ignored": "skipped",
            },
        )

    def test_a_dtd_is_refused_and_leaves_the_cases_unexecuted(self):
        with scratch_dir() as root:
            junit = root / runner.JUNIT_DIR
            junit.mkdir()
            self.write(junit, "TEST-x.xml", '<!DOCTYPE x [<!ENTITY y "z">]><testsuite/>')
            result = runner.RunResult(exit_code=0)
            runner._read_reports(root, MANIFEST, result, "crash")
        self.assertEqual(result.outcomes, {})
        self.assertTrue(any(e.startswith("junit: unreadable") for e in result.read_errors))

    def test_the_perf_suite_does_not_read_junit(self):
        with scratch_dir() as root:
            (root / runner.JUNIT_DIR).mkdir()
            self.write(root / runner.JUNIT_DIR, "TEST-x.xml", self.SUITE.format(cls="a.One"))
            result = runner.RunResult(exit_code=0)
            runner._read_reports(root, MANIFEST, result, "perf")
        self.assertEqual(result.outcomes, {})


class FixtureRunnerTests(unittest.TestCase):
    def test_exit_code_and_failure_lines_come_from_run_json(self):
        fixtures = runner.FixtureRunner(FIXTURES / "reports", MANIFEST, "baseline", "bad-parity")
        result = fixtures.run("candidate", FIXTURES, "x", "spark-3.5", 1.0, "perf")
        self.assertEqual(result.exit_code, 1)
        self.assertTrue(any("*** FAILED ***" in line for line in result.failure_lines))
        self.assertIsNotNone(result.perf)
        self.assertEqual(fixtures.run("baseline", FIXTURES, "x", "spark-3.5", 1.0, "perf").exit_code, 0)
        self.assertEqual(fixtures.run("baseline", FIXTURES, "x", "spark-3.5", 1.0, "crash").exit_code, 1)

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

    def test_destructive_git_operations_only_ever_touch_the_evaluator_cache_worktree(self):
        cache = self.tmp / "cache"
        with self.assertRaisesRegex(runner.InfraError, "not the evaluator baseline worktree"):
            runner.assert_owned_worktree(cache, self.source, self.first)
        with self.assertRaisesRegex(runner.InfraError, "non-SHA"):
            runner.assert_owned_worktree(cache, cache / "baseline" / self.first[:12], "HEAD")
        with self.assertRaisesRegex(runner.InfraError, "not the evaluator baseline worktree"):
            runner.assert_owned_worktree(cache, cache / "baseline" / "other", self.first)
        owned = runner.prepare_baseline_worktree(cache, self.first, [str(self.source)])
        self.assertEqual(runner.assert_owned_worktree(cache, owned, self.first), owned.resolve())

    def test_a_lane_checkout_in_the_cache_path_is_never_reset(self):
        cache = self.tmp / "cache"
        lane = cache / "baseline" / self.first[:12]
        lane.mkdir(parents=True)
        self.git(lane, "init", "--quiet")
        (lane / "work.txt").write_text("uncommitted lane work\n")
        with self.assertRaisesRegex(runner.InfraError, "ownership marker"):
            runner.prepare_baseline_worktree(cache, self.first, [str(self.source)])
        self.assertEqual((lane / "work.txt").read_text(), "uncommitted lane work\n")

    def test_a_non_empty_directory_without_git_and_a_protected_checkout_are_refused(self):
        cache = self.tmp / "cache"
        stray = cache / "baseline" / self.first[:12]
        stray.mkdir(parents=True)
        (stray / "keep.txt").write_text("x")
        with self.assertRaisesRegex(runner.InfraError, "not empty"):
            runner.prepare_baseline_worktree(cache, self.first, [str(self.source)])
        self.assertTrue((stray / "keep.txt").exists())
        inside = self.tmp / "lane"
        with self.assertRaisesRegex(runner.InfraError, "overlaps the protected checkout"):
            runner.prepare_baseline_worktree(inside / "cache", self.first, [str(self.source)], protect=(inside,))

    def test_an_abbreviated_baseline_is_refused_before_touching_the_disk(self):
        cache = self.tmp / "cache"
        with self.assertRaises(runner.InfraError):
            runner.prepare_baseline_worktree(cache, self.first[:12], [str(self.source)])
        self.assertFalse(cache.exists())

    def test_resolve_baseline_prefers_the_local_object_then_fetches(self):
        self.assertEqual(policy.resolve_baseline(self.source, "HEAD~1"), self.first)
        clone = self.tmp / "clone"
        self.git(self.tmp, "clone", "--quiet", "--depth=1", self.source.as_uri(), str(clone))
        self.assertEqual(policy.resolve_baseline(clone, self.second), self.second)
        with self.assertRaises(policy.PolicyError):
            policy.resolve_baseline(clone, "refs/heads/does-not-exist")


if __name__ == "__main__":
    unittest.main()
