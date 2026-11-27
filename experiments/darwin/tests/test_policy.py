import os
import subprocess
import unittest

import policy
from . import MANIFEST, scratch_dir

BARRIER = MANIFEST["lanes"]["a"]["owned"][0]
MVC = MANIFEST["lanes"]["a"]["shared"][0]
TEST_FILE = "spark-ext/ivm-it/src/test/scala/org/openivm/spark/Existing.scala"
GIT = ["git", "-c", "user.name=t", "-c", "user.email=t@example.invalid", "-c", "commit.gpgsign=false"]

CM = "spark-ext/ivm-common/src/main/scala/org/openivm/spark/common/"
RDB = CM + "rocksdb/"
COMMANDS = "spark-ext/ivm-extension/src/main/scala/org/openivm/spark/commands/MaterializedViewCommands.scala"


def change(path, status="M", old_mode="100644", new_mode="100644", binary=False):
    return {"path": path, "status": status, "old_mode": old_mode, "new_mode": new_mode, "binary": binary}


# Literal paths, independent of manifest ordering: name -> (lane, changes, ok, violation needles, shared touched).
DIFF_CASES = {
    "a-owned-and-shared": ("a", [change(RDB + "OpenIvmStateBarrier.scala"), change(COMMANDS)], True, [], 1),
    "a-touches-b-file": ("a", [change(CM + "CdfWatermarkBackend.scala")], False, ["not in lane A allowlist"], 0),
    "b-shared-delta-backend": ("b", [change(CM + "DeltaMvCatalogBackend.scala")], True, [], 1),
    "binary-owned": ("a", [change(RDB + "OpenIvmStateBarrier.scala", binary=True)], False, ["binary blob"], 0),
    "deleted-owned": ("a", [change(RDB + "OpenIvmStateBarrier.scala", status="D")], False, ["deleted"], 0),
    "e-new-files": (
        "e",
        [change(RDB + "OpenIvmStateDeltaWriter.scala", "A"), change(RDB + "OpenIvmStateSyncCodecV2.scala", "A")],
        True,
        [],
        0,
    ),
    "e-new-wrong-name": (
        "e", [change(RDB + "OpenIvmHelper.scala", "A")], False, ["new file outside lane E allowlist"], 0
    ),
    "frozen-build-and-experiments": (
        "c",
        [change("spark-ext/build.sbt"), change("experiments/darwin/manifest.json")],
        False,
        ["spark-ext/build.sbt: frozen", "experiments/darwin/manifest.json: frozen"],
        0,
    ),
    "frozen-telemetry": ("a", [change(CM + "telemetry/OpenIvmExecutionSpan.scala")], False, ["frozen"], 0),
    "frozen-test-file": (
        "a",
        [change("spark-ext/ivm-it/src/test/scala/org/openivm/spark/perf/StateGuardPerfSpec.scala")],
        False,
        ["frozen"],
        0,
    ),
    "gitattributes": ("a", [change(".gitattributes", "A")], False, ["frozen"], 0),
    "mode-change": ("a", [change(RDB + "OpenIvmStateBarrier.scala", new_mode="100755")], False, ["mode change"], 0),
    "new-file-lane-a": (
        "a", [change(RDB + "OpenIvmNewHelper.scala", "A")], False, ["new file outside lane A allowlist"], 0
    ),
    "path-traversal": ("a", [change("../outside.scala", "A")], False, ["unsafe path"], 0),
    "protocol-files": ("a", [change("output/ideas.json", "A"), change("output/notes.md", "A")], True, [], 0),
    "symlink-new": (
        "e",
        [change(RDB + "OpenIvmStateDeltaWriter.scala", "A", old_mode="000000", new_mode="120000")],
        False,
        ["symlink"],
        0,
    ),
}


def git(root, *args):
    return subprocess.run([*GIT, *args], cwd=root, check=True, capture_output=True, text=True).stdout.strip()


def write(root, rel, data):
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data if isinstance(data, bytes) else data.encode())


class DiffCaseTests(unittest.TestCase):
    def test_every_diff_case_matches_the_expected_verdict(self):
        for name, (lane, changes, ok, needles, shared_touched) in DIFF_CASES.items():
            with self.subTest(case=name):
                verdict = policy.check_changes(changes, MANIFEST, lane)
                self.assertEqual(verdict["ok"], ok, verdict["violations"])
                self.assertEqual(verdict["shared_touched"], shared_touched)
                for needle in needles:
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
