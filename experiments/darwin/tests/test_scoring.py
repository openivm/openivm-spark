import copy
import unittest

import scoring
from . import MANIFEST, perf


def compute(lane, candidate, pairs=3, baseline="baseline"):
    return scoring.compute(lane, MANIFEST, [perf(baseline) for _ in range(pairs)], [perf(candidate) for _ in range(pairs)])


def scaled(doc, field, factor):
    for view in doc["views"]:
        view[field] *= factor
    return doc


class SaneTests(unittest.TestCase):
    def test_non_finite_and_absurd_values_are_clamped(self):
        self.assertEqual(scoring.sane(float("nan")), 0.0)
        self.assertEqual(scoring.sane(float("inf")), 0.0)
        self.assertEqual(scoring.sane(1e301), 1e12)
        self.assertEqual(scoring.sane(-1e301), -1e12)
        self.assertEqual(scoring.sane(12.5), 12.5)


class CombinedScoreTests(unittest.TestCase):
    def test_formula_and_accepted_bonus(self):
        self.assertAlmostEqual(scoring.combined_score(0.2, 0.9, True, MANIFEST), 20 + 45 + 25)
        self.assertAlmostEqual(scoring.combined_score(0.2, 0.9, False, MANIFEST), 65)

    def test_ratios_are_clamped(self):
        self.assertAlmostEqual(scoring.combined_score(5.0, 5.0, True, MANIFEST), 100 + 50 + 25)

    def test_valid_but_slower_candidates_stay_above_the_invalid_zero(self):
        self.assertEqual(scoring.combined_score(-0.5, -0.5, False, MANIFEST), MANIFEST["scoring"]["valid_floor"])
        self.assertGreater(scoring.combined_score(-1.0, -1.0, False, MANIFEST), 0.0)


class PairedMedianTests(unittest.TestCase):
    def test_e2e_improvement_is_the_median_of_the_paired_ratios(self):
        baselines = [perf("baseline") for _ in range(3)]
        candidates = [perf("good-candidate") for _ in range(3)]
        for doc, makespan in zip(baselines, (100.0, 200.0, 400.0)):
            doc["makespan_ms"] = doc["end_to_end_ms"] = makespan
        for doc, makespan in zip(candidates, (80.0, 190.0, 100.0)):
            doc["makespan_ms"] = doc["end_to_end_ms"] = makespan
        scores = scoring.compute("a", MANIFEST, baselines, candidates)
        self.assertAlmostEqual(scores.e2e_improvement_median, 0.20)
        self.assertAlmostEqual(scores.e2e_improvement_min, 0.05)
        self.assertAlmostEqual(scores.e2e_improvement_max, 0.75)
        self.assertEqual((scores.baseline_e2e, scores.candidate_e2e), (200.0, 100.0))

    def test_a_single_lucky_pair_does_not_make_a_candidate_accepted(self):
        baselines = [perf("baseline") for _ in range(3)]
        candidates = [perf("good-candidate") for _ in range(3)]
        for doc in candidates[1:]:
            doc["makespan_ms"] = doc["end_to_end_ms"] = baselines[0]["makespan_ms"] * 0.97
        scores = scoring.compute("a", MANIFEST, baselines, candidates)
        self.assertGreater(scores.e2e_improvement_max, 0.5)
        self.assertLess(scores.e2e_improvement_median, 0.10)
        self.assertFalse(scores.accepted)

    def test_pair_count_mismatch_and_profile_mismatch_are_errors(self):
        base = [perf("baseline")] * 3
        self.assertTrue(scoring.compute("a", MANIFEST, base, base[:2]).errors)
        other = perf("good-candidate")
        other["latency_profile"]["scale"] += 1
        self.assertIn("latency_profile", scoring.compute("a", MANIFEST, base, [other] * 3).errors[0])

    def test_zero_baseline_wait_cannot_be_scored(self):
        zero = scaled(perf("baseline"), "catalog_finalize_ms", 0.0)
        scores = scoring.compute("b", MANIFEST, [zero] * 3, [zero] * 3)
        self.assertIn("baseline targeted wait is zero", scores.errors[0])


class AcceptanceTests(unittest.TestCase):
    def test_good_candidate_is_accepted_on_every_lane(self):
        for lane in "abcde":
            with self.subTest(lane=lane):
                scores = compute(lane, "good-candidate")
                self.assertEqual(scores.errors, [])
                self.assertEqual(scores.concurrency_failures, [])
                self.assertGreaterEqual(scores.wait_reduction, 0.8)
                self.assertGreaterEqual(scores.e2e_improvement_median, 0.1)
                self.assertTrue(scores.accepted)

    def test_each_lane_only_credits_its_own_targeted_waits(self):
        expected_accepted = {"a": True, "b": False, "c": False, "d": False, "e": False}
        for lane, accepted in expected_accepted.items():
            with self.subTest(lane=lane):
                scores = compute(lane, "lane-a-only")
                self.assertEqual(scores.accepted, accepted)
                if lane in "bce":
                    self.assertEqual(scores.wait_reduction, 0.0)

    def test_wait_reduction_threshold(self):
        for factor, accepted in ((0.19, True), (0.21, False)):
            candidate = perf("good-candidate")
            baseline = perf("baseline")
            total = sum(v["catalog_finalize_ms"] for v in baseline["views"])
            for view in candidate["views"]:
                view["catalog_finalize_ms"] = 0.0
            candidate["views"][0]["catalog_finalize_ms"] = total * factor
            with self.subTest(remaining=factor):
                scores = scoring.compute("b", MANIFEST, [baseline] * 3, [candidate] * 3)
                self.assertEqual(scores.accepted, accepted, scores.wait_reduction)

    def test_e2e_threshold(self):
        for improvement, accepted in ((0.11, True), (0.09, False)):
            candidate = perf("good-candidate")
            baseline = perf("baseline")
            candidate["makespan_ms"] = candidate["end_to_end_ms"] = baseline["makespan_ms"] * (1 - improvement)
            with self.subTest(improvement=improvement):
                scores = scoring.compute("a", MANIFEST, [baseline] * 3, [candidate] * 3)
                self.assertEqual(scores.accepted, accepted)

    def test_slow_candidate_is_negative_and_never_accepted(self):
        scores = compute("a", "slow-valid")
        self.assertLess(scores.e2e_improvement_median, 0)
        self.assertLess(scores.wait_reduction, 0)
        self.assertFalse(scores.accepted)

    def test_serializing_the_engines_is_not_a_speedup(self):
        scores = compute("a", "serialized-engines")
        self.assertGreaterEqual(scores.wait_reduction, 0.8)
        self.assertTrue(any("serialized" in f for f in scores.concurrency_failures))
        self.assertFalse(scores.accepted)

    def test_deferring_work_past_the_interval_is_not_a_speedup(self):
        scores = compute("a", "deferred-work")
        self.assertTrue(any("moved outside the measured interval" in f for f in scores.concurrency_failures))
        self.assertFalse(scores.accepted)


class MetricSurfaceTests(unittest.TestCase):
    def test_emitted_metric_names_cover_exactly_what_compute_produces(self):
        for lane in "abcde":
            with self.subTest(lane=lane):
                scores = compute(lane, "good-candidate")
                self.assertEqual(set(scores.metrics), set(scoring.metric_names(lane, MANIFEST)))

    def test_per_mv_raw_outputs_are_first_class(self):
        names = scoring.metric_names("a", MANIFEST)
        for index in range(1, 7):
            for field in ("submit_to_ack_ms", "state_barrier_hold_ms", "state_barrier_wait_ms", "capture_wait_ms", "publication_queue_wait_ms"):
                self.assertIn(f"mv{index}_{field}", names)

    def test_lane_specific_informational_metrics(self):
        scores = compute("d", "good-candidate")
        self.assertGreater(scores.metrics["info_catalog_fs_ops_total_baseline"], 0)
        scores = compute("e", "good-candidate")
        self.assertLess(scores.metrics["info_state_sync_write_ops_candidate"], scores.metrics["info_state_sync_write_ops_baseline"])
        self.assertAlmostEqual(scores.wait_reduction, 0.9)

    def test_scoring_does_not_mutate_its_inputs(self):
        baselines, candidates = [perf("baseline")] * 3, [perf("good-candidate")] * 3
        before = copy.deepcopy((baselines, candidates))
        scoring.compute("c", MANIFEST, baselines, candidates)
        self.assertEqual((baselines, candidates), before)


if __name__ == "__main__":
    unittest.main()
