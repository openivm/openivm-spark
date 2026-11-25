import json
import os
import subprocess
import unittest

import policy
from . import FIXTURES, MANIFEST, scratch_dir

BARRIER = MANIFEST["lanes"]["a"]["owned"][0]
MVC = MANIFEST["lanes"]["a"]["shared"][0]
TEST_FILE = "spark-ext/ivm-it/src/test/scala/org/openivm/spark/Existing.scala"
GIT = ["git", "-c", "user.name=t", "-c", "user.email=t@example.invalid", "-c", "commit.gpgsign=false"]


def git(root, *args):
    return subprocess.run([*GIT, *args], cwd=root, check=True, capture_output=True, text=True).stdout.strip()


def write(root, rel, data):
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data if isinstance(data, bytes) else data.encode())


class DiffFixtureTests(unittest.TestCase):
    def test_every_diff_fixture_matches_the_expected_verdict(self):
        cases = sorted((FIXTURES / "diffs").glob("*.json"))
        self.assertGreaterEqual(len(cases), 12)
        for path in cases:
            case = json.loads(path.read_text(encoding="utf-8"))
            with self.subTest(case=path.stem):
                verdict = policy.check_changes(case["changes"], MANIFEST, case["lane"])
                self.assertEqual(verdict["ok"], case["ok"], verdict["violations"])
                self.assertEqual(verdict["shared_touched"], case["shared_touched"])
                for needle in case["violations"]:
                    self.assertTrue(any(needle in v for v in verdict["violations"]), (needle, verdict["violations"]))

    def test_shared_file_warning_names_the_other_lanes(self):
        verdict = policy.check_changes(
            [{"path": MVC, "status": "M", "old_mode": "100644", "new_mode": "100644"}], MANIFEST, "a"
        )
        self.assertTrue(verdict["ok"])
        self.assertIn("also used by lane(s) B, C, D", verdict["warnings"][0])

    def test_frozen_matching_is_case_insensitive_and_checks_every_path_segment(self):
        for path in (
            "SPARK-EXT/BUILD.SBT",
            "spark-ext/ivm-common/src/main/scala/StateGuardPerfSpec.scala",
            "spark-ext/ivm-common/src/main/scala/org/openivm/spark/common/state-guard-helper/Foo.scala",
            ".GitAttributes",
        ):
            with self.subTest(path=path):
                self.assertTrue(policy.is_frozen(path, MANIFEST))
        self.assertFalse(policy.is_frozen(BARRIER, MANIFEST))


class CollectChangesTests(unittest.TestCase):
    """Real git: the changed set must include everything a candidate can add, delete or retype."""

    def setUp(self):
        context = scratch_dir()
        self.root = context.__enter__()
        self.addCleanup(context.__exit__, None, None, None)
        git(self.root, "init", "--quiet")
        self.assertTrue((self.root / ".git").is_dir())
        write(self.root, ".gitignore", "Hidden*.scala\n")
        write(self.root, BARRIER, "object B\n")
        write(self.root, MVC, "object M\n")
        write(self.root, TEST_FILE, "object T\n")
        git(self.root, "add", "-A")
        git(self.root, "commit", "--quiet", "-m", "baseline")
        self.base = git(self.root, "rev-parse", "HEAD")

    def changes(self):
        return {c["path"]: c for c in policy.collect_changes(self.root, self.base)}

    def test_no_changes(self):
        self.assertEqual(policy.collect_changes(self.root, self.base), [])

    def test_modified_deleted_and_untracked_files(self):
        write(self.root, BARRIER, "object B2\n")
        (self.root / MVC).unlink()
        write(self.root, "spark-ext/ivm-common/src/main/scala/New.scala", "object N\n")
        found = self.changes()
        self.assertEqual({p: c["status"] for p, c in found.items()}, {
            BARRIER: "M",
            MVC: "D",
            "spark-ext/ivm-common/src/main/scala/New.scala": "A",
        })
        verdict = policy.check_changes(list(found.values()), MANIFEST, "a")
        self.assertFalse(verdict["ok"])

    def test_committed_candidate_changes_are_still_seen(self):
        write(self.root, BARRIER, "object B2\n")
        git(self.root, "commit", "--quiet", "-am", "candidate")
        self.assertEqual(list(self.changes()), [BARRIER])

    def test_gitignored_file_under_src_is_not_hidden(self):
        hidden = "spark-ext/ivm-common/src/main/scala/HiddenHelper.scala"
        write(self.root, hidden, "object H\n")
        self.assertEqual(self.changes()[hidden]["status"], "A")

    def test_binary_blobs_are_flagged_tracked_and_untracked(self):
        write(self.root, BARRIER, b"\x00\x01\x02binary")
        write(self.root, "spark-ext/ivm-common/src/main/scala/blob.bin", b"\x00\x00\x00")
        found = self.changes()
        self.assertTrue(found[BARRIER]["binary"])
        self.assertTrue(found["spark-ext/ivm-common/src/main/scala/blob.bin"]["binary"])

    def test_mode_change_is_reported(self):
        os.chmod(self.root / BARRIER, 0o755)
        git(self.root, "update-index", "--chmod=+x", BARRIER)
        change = self.changes()[BARRIER]
        self.assertEqual((change["old_mode"], change["new_mode"]), ("100644", "100755"))
        self.assertFalse(policy.check_changes([change], MANIFEST, "a")["ok"])

    def test_untracked_symlink_is_reported(self):
        link = self.root / "spark-ext" / "ivm-common" / "src" / "main" / "scala" / "link.scala"
        link.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.symlink(self.root / BARRIER, link)
        except (OSError, NotImplementedError):
            self.skipTest("symlinks are not available on this platform")
        change = self.changes()["spark-ext/ivm-common/src/main/scala/link.scala"]
        self.assertEqual(change["new_mode"], policy.SYMLINK_MODE)
        self.assertIn("symlink", policy.check_changes([change], MANIFEST, "e")["violations"][0])


if __name__ == "__main__":
    unittest.main()
