import importlib.util
import io
import re
import unittest

import state_guard_eval
from . import DARWIN, FIXTURES, MANIFEST, make_fixtures, scratch_dir

CONFIGS = DARWIN / "configs"
LANES = "abcde"
SAMPLE_LANE = "a"
CONFIG_LANES = sorted(path.stem.removeprefix("lane-") for path in CONFIGS.glob("lane-*.yaml"))
UNIVERSAL_RULES = (
    "FULL_REFRESH",
    "nested and NOT additive",
    "NOT proven",
    "Do not weaken",
    "crash-safety",
    "serialize",
)


def config_text(lane):
    return (CONFIGS / f"lane-{lane}.yaml").read_text(encoding="utf-8")


def editable_paths(lane):
    spec = MANIFEST["lanes"][lane]
    return spec["owned"] + spec["shared"] + spec["new_files"]


def metrics_block(text):
    block = text.split("\n      metrics:\n", 1)[1].split("\n      artifacts:\n", 1)[0]
    return re.findall(r"^        (\w+): !eval (.+)$", block, re.MULTILINE)


def evaluator_command(text):
    return re.search(r"(?m)^      commands:\n        - (.+)$", text).group(1)


class LaneConfigTests(unittest.TestCase):
    def test_the_sample_and_every_local_lane_config_name_their_own_lane(self):
        self.assertIn(SAMPLE_LANE, CONFIG_LANES)
        self.assertLessEqual(set(CONFIG_LANES), set(LANES))
        for lane in CONFIG_LANES:
            text = config_text(lane)
            with self.subTest(lane=lane):
                self.assertRegex(text, rf"(?m)^experiments:\n  - name: openivm-state-guard-lane-{lane}$")
                self.assertIn(f"lane {lane.upper()} of five", text)
                self.assertNotRegex(text, r"(?m)^(source|container|samplers|name):", "obsolete `darwin run` schema")

    def test_budget_and_evaluator_settings_match_the_manifest(self):
        budget = MANIFEST["budget"]
        for lane in CONFIG_LANES:
            text = config_text(lane)
            with self.subTest(lane=lane):
                self.assertRegex(text, rf"(?m)^        max_iterations: {budget['max_iterations']}$")
                self.assertRegex(text, rf"(?m)^          max_iterations_without_improvement: {budget['max_iterations_without_improvement']}$")
                self.assertRegex(text, rf"(?m)^          metric: {budget['metric']}$")
                self.assertRegex(text, rf"(?m)^          min_improvement: {budget['min_improvement']}$")
                self.assertRegex(text, rf"(?m)^          improvement_type: {budget['improvement_type']}$")
                self.assertRegex(text, rf"(?m)^      - timeout: {budget['agent_timeout_s']}$")
                self.assertRegex(text, rf"(?m)^          - name: {re.escape(budget['model'])}$")
                self.assertRegex(text, rf"(?m)^      timeout: {budget['iteration_timeout_s']}$")
        self.assertGreater(budget["iteration_timeout_s"], budget["deadline_s"])
        self.assertGreaterEqual(budget["deadline_s"], budget["crash_timeout_s"])

    def test_command_runs_the_trusted_evaluator_for_this_lane_with_pinned_inputs(self):
        for lane in CONFIG_LANES:
            command = evaluator_command(config_text(lane))
            with self.subTest(lane=lane):
                self.assertIn('"${OPENIVM_DARWIN_TRUSTED:?', command)
                self.assertIn("/experiments/darwin/evaluator/state_guard_eval.py", command)
                self.assertIn("--mode candidate", command)
                self.assertIn(f"--lane {lane} ", command)
                self.assertIn('--workspace "$PWD"', command)
                self.assertIn('--baseline-repo "${OPENIVM_DARWIN_TRUSTED}"', command)
                self.assertIn('--baseline-ref "${OPENIVM_DARWIN_BASELINE_SHA:?', command)
                self.assertIn('--lock-sha256 "${OPENIVM_DARWIN_LOCK_SHA256:?', command)
                self.assertIn(f"--pairs {MANIFEST['acceptance']['default_pairs']}", command)
                for forbidden in ("--skip-lock-check", "--no-box-lock", "--offline", "--manifest"):
                    self.assertNotIn(forbidden, command)

    def test_every_metric_the_evaluator_emits_is_declared_and_parses_its_own_output(self):
        for lane in CONFIG_LANES:
            out = state_guard_eval.Output(MANIFEST, lane)
            for index, name in enumerate(out.names):
                out.results[name] = index + 0.25
            stream = io.StringIO()
            out.emit(stream)
            stdout = [line.strip() for line in stream.getvalue().splitlines() if line.strip()]
            declared = metrics_block(config_text(lane))
            with self.subTest(lane=lane):
                self.assertEqual([name for name, _ in declared], out.names)
                for name, expression in declared:
                    if name.endswith("_gate"):
                        continue
                    self.assertEqual(eval(expression, {}, {"stdout": stdout}), float(out.results[name]), name)

    def test_missing_evaluator_output_fails_the_evaluation_instead_of_scoring_zero(self):
        name, expression = metrics_block(config_text(SAMPLE_LANE))[0]
        self.assertEqual(name, "combined_score")
        with self.assertRaises(IndexError):
            eval(expression, {}, {"stdout": ["@FEEDBACK infra error"]})

    def test_feedback_artifact_joins_the_feedback_lines(self):
        match = re.search(r"(?m)^        feedback: !eval '(.+)'$", config_text(SAMPLE_LANE))
        stdout = ["@FEEDBACK one", "@RESULT combined_score=1.0", "@FEEDBACK two"]
        self.assertEqual(eval(match.group(1), {}, {"stdout": stdout}), "one\ntwo")

    def test_configs_carry_no_credentials_hosts_placeholders_or_odd_whitespace(self):
        for lane in CONFIG_LANES:
            text = config_text(lane)
            with self.subTest(lane=lane):
                self.assertNotRegex(text, r"gh[pousr]_[A-Za-z0-9]{20}|Bearer |msft\.ghe\.com|COPILOT_GITHUB_TOKEN=")
                self.assertNotRegex(text, r'"<[A-Za-z0-9_]+>"')
                self.assertNotIn("\r", text)
                self.assertNotIn("\t", text)

    def test_configs_load_as_darwin_evolve_manifests_when_pyyaml_is_available(self):
        yaml = importlib.util.find_spec("yaml")
        if yaml is None:
            self.skipTest("PyYAML is not installed")
        import yaml as pyyaml

        class Loader(pyyaml.SafeLoader):
            pass

        Loader.add_constructor("!eval", lambda loader, node: loader.construct_scalar(node))
        for lane in CONFIG_LANES:
            doc = pyyaml.load(config_text(lane), Loader=Loader)
            with self.subTest(lane=lane):
                experiment = doc["experiments"][0]
                self.assertEqual(set(experiment), {"name", "executor", "agents", "evaluator", "prompt"})
                self.assertEqual(set(experiment["prompt"]), {"system", "agent"})
                self.assertEqual(len(experiment["evaluator"]["metrics"]), len(metrics_block(config_text(lane))))


class LanePromptTests(unittest.TestCase):
    def test_prompt_lists_exactly_the_manifest_editable_files_of_its_lane(self):
        for lane in CONFIG_LANES:
            text = config_text(lane)
            with self.subTest(lane=lane):
                for path in editable_paths(lane):
                    self.assertIn(path, text)
                own = set(editable_paths(lane))
                for other in LANES:
                    if other == lane:
                        continue
                    for path in editable_paths(other):
                        if path not in own:
                            self.assertNotIn(path, text, f"lane {lane} prompt leaks lane {other}'s file")

    def test_prompt_states_the_acceptance_criteria_and_universal_rules(self):
        acceptance = MANIFEST["acceptance"]
        for lane in CONFIG_LANES:
            text = config_text(lane)
            with self.subTest(lane=lane):
                self.assertIn(f"at least {round(acceptance['min_wait_reduction'] * 100)}%", text)
                self.assertIn(f"at least {round(acceptance['min_e2e_improvement'] * 100)}%", text)
                for rule in UNIVERSAL_RULES:
                    self.assertIn(rule, text)

    def test_prompt_names_the_lane_target_metric(self):
        for lane in CONFIG_LANES:
            targeted = MANIFEST["lanes"][lane]["targeted"]
            fields = targeted["fields"] if targeted["kind"] == "view_fields" else [targeted["path"]]
            with self.subTest(lane=lane):
                for field in fields:
                    self.assertIn(field, config_text(lane))

    def test_only_lanes_that_may_touch_the_known_gap_mention_it_as_in_scope(self):
        may_touch = set(MANIFEST["known_gap"]["lanes_that_may_touch"])
        for lane in CONFIG_LANES:
            with self.subTest(lane=lane):
                self.assertEqual("This lane may touch that path" in config_text(lane), lane in may_touch)


class ManifestMirrorTests(unittest.TestCase):
    def test_human_readable_manifest_lists_every_editable_path_and_frozen_glob(self):
        mirror = (DARWIN / "manifest.yaml").read_text(encoding="utf-8")
        for lane in LANES:
            for path in editable_paths(lane):
                with self.subTest(lane=lane, path=path):
                    self.assertIn(path, mirror)
        for glob in MANIFEST["frozen"]["globs"] + MANIFEST["frozen"]["basename_globs"]:
            with self.subTest(glob=glob):
                self.assertIn(glob, mirror)

    def test_every_allowlisted_existing_file_exists_in_the_repository(self):
        repo = DARWIN.parent.parent
        for lane in LANES:
            for path in MANIFEST["lanes"][lane]["owned"] + MANIFEST["lanes"][lane]["shared"]:
                with self.subTest(path=path):
                    self.assertTrue((repo / path).is_file())

    def test_no_lane_allowlist_contains_a_frozen_file(self):
        import policy

        for lane in LANES:
            for path in editable_paths(lane):
                with self.subTest(lane=lane, path=path):
                    self.assertFalse(policy.is_frozen(path, MANIFEST))

    def test_lanes_own_the_files_their_candidates_change(self):
        catalog = "spark-ext/ivm-common/src/main/scala/org/openivm/spark/common/"
        self.assertIn(catalog + "DeltaPathTable.scala", MANIFEST["lanes"]["b"]["owned"])
        for lane in "abd":
            self.assertIn(catalog + "MvCatalog.scala", editable_paths(lane))
        self.assertIn(catalog + "DeltaPathTable.scala", editable_paths("d"))


class FixtureReproducibilityTests(unittest.TestCase):
    def test_generated_reports_are_reproducible(self):
        self.assertEqual(make_fixtures.check(FIXTURES), [])

    def test_output_directory_keeps_unrelated_files_and_detects_report_drift(self):
        with scratch_dir() as root:
            unrelated = root / "notes" / "keep.json"
            unrelated.parent.mkdir()
            unrelated.write_text('{"ok": false}\n', encoding="utf-8")
            self.assertEqual(make_fixtures.main(["--output-dir", str(root)]), 0)
            self.assertEqual(make_fixtures.main(["--output-dir", str(root), "--check"]), 0)
            self.assertEqual(unrelated.read_text(encoding="utf-8"), '{"ok": false}\n')
            (root / "reports" / "bad-crash" / "run.json").unlink()
            (root / "reports" / "bad-parity" / "run.json").write_text("{}\n", encoding="utf-8")
            (root / "reports" / "unexpected.json").write_text("{}\n", encoding="utf-8")
            self.assertCountEqual(make_fixtures.check(root), [
                "reports/bad-crash/run.json: missing",
                "reports/bad-parity/run.json: stale",
                "reports/unexpected.json: unexpected file",
            ])


if __name__ == "__main__":
    unittest.main()
