import importlib.util
import re
import unittest

import darwin_evaluator
from . import DARWIN, FIXTURES, MANIFEST

CONFIGS = DARWIN / "configs"
LANES = "abcde"
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


def evaluator_option_keys(text):
    block = text.split("\nevaluator:\n", 1)[1].split("\nagents:\n", 1)[0]
    options = block.split("  options:\n", 1)[1]
    return re.findall(r"^    (\w+):", options, re.MULTILINE)


class LaneConfigTests(unittest.TestCase):
    def test_every_lane_has_a_config_that_names_its_own_lane(self):
        self.assertEqual(sorted(p.name for p in CONFIGS.glob("lane-*.yaml")), [f"lane-{x}.yaml" for x in LANES])
        for lane in LANES:
            text = config_text(lane)
            with self.subTest(lane=lane):
                self.assertRegex(text, rf"(?m)^name: openivm-state-guard-lane-{lane}$")
                self.assertRegex(text, rf"(?m)^    lane: {lane}$")
                self.assertIn(f"lane {lane.upper()} of five", text)

    def test_budget_and_evaluator_settings_match_the_manifest(self):
        budget = MANIFEST["budget"]
        for lane in LANES:
            text = config_text(lane)
            with self.subTest(lane=lane):
                self.assertRegex(text, rf"(?m)^    max_iterations: {budget['max_iterations']}$")
                self.assertRegex(text, rf"(?m)^      max_iterations_without_improvement: {budget['max_iterations_without_improvement']}$")
                self.assertRegex(text, rf"(?m)^      metric: {budget['metric']}$")
                self.assertRegex(text, rf"(?m)^      min_improvement: {budget['min_improvement']}$")
                self.assertRegex(text, rf"(?m)^      improvement_type: {budget['improvement_type']}$")
                self.assertRegex(text, rf"(?m)^  timeout: {budget['iteration_timeout_s']}$")
                self.assertRegex(text, r"(?m)^  retries: 0$")
                self.assertRegex(text, r"(?m)^  path: \.\./evaluator/darwin_evaluator\.py$")
                self.assertRegex(text, rf"(?m)^    pairs: {MANIFEST['acceptance']['default_pairs']}$")

    def test_evaluator_options_are_all_declared_by_the_wrapper(self):
        declared = set(darwin_evaluator.StateGuardOptions.__annotations__)
        for lane in LANES:
            with self.subTest(lane=lane):
                keys = evaluator_option_keys(config_text(lane))
                self.assertTrue({"lane", "baseline_ref", "lock_sha256"} <= set(keys), keys)
                self.assertLessEqual(set(keys), declared)

    def test_configs_pin_the_trusted_evaluator_and_are_not_candidate_editable(self):
        self.assertTrue((CONFIGS / "../evaluator/darwin_evaluator.py").resolve().is_file())
        for lane in LANES:
            text = config_text(lane)
            with self.subTest(lane=lane):
                self.assertIn("<SHA256_OF_evaluator.lock.json>", text)
                self.assertIn("experiments/", text)
                self.assertNotRegex(text, r"gh[pousr]_[A-Za-z0-9]{20}|Bearer ")
                self.assertNotIn("\r", text)
                self.assertNotIn("\t", text)


class LanePromptTests(unittest.TestCase):
    def test_prompt_lists_exactly_the_manifest_editable_files_of_its_lane(self):
        for lane in LANES:
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
        for lane in LANES:
            text = config_text(lane)
            with self.subTest(lane=lane):
                self.assertIn(f"at least {round(acceptance['min_wait_reduction'] * 100)}%", text)
                self.assertIn(f"at least {round(acceptance['min_e2e_improvement'] * 100)}%", text)
                for rule in UNIVERSAL_RULES:
                    self.assertIn(rule, text)

    def test_prompt_names_the_lane_target_metric(self):
        for lane in LANES:
            targeted = MANIFEST["lanes"][lane]["targeted"]
            fields = targeted["fields"] if targeted["kind"] == "view_fields" else [targeted["path"]]
            with self.subTest(lane=lane):
                for field in fields:
                    self.assertIn(field, config_text(lane))

    def test_only_lanes_that_may_touch_the_known_gap_mention_it_as_in_scope(self):
        may_touch = set(MANIFEST["known_gap"]["lanes_that_may_touch"])
        for lane in LANES:
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


class FixtureReproducibilityTests(unittest.TestCase):
    def test_committed_fixtures_are_what_the_generator_produces(self):
        spec = importlib.util.spec_from_file_location("make_fixtures", FIXTURES / "make_fixtures.py")
        generator = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(generator)
        self.assertEqual(generator.check(), [])


if __name__ == "__main__":
    unittest.main()
