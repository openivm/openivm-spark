import copy
import unittest

import report
from . import MANIFEST, crash, perf, scratch_dir

TARGET = "spark-3.5"


def problems(doc):
    return report.validate_perf(doc, TARGET, MANIFEST)


class PerfValidationTests(unittest.TestCase):
    def test_structurally_valid_fixtures_validate_cleanly(self):
        for name in ("baseline", "good-candidate", "slow-valid", "bad-parity", "full-refresh-regression", "serialized-engines", "deferred-work"):
            with self.subTest(report=name):
                self.assertEqual(problems(perf(name)), [])

    def test_wrong_evidence_class_and_spark_target_are_rejected(self):
        self.assertIn("evidence_class", " ".join(problems(perf("wrong-evidence-class"))))
        self.assertTrue(any(p.startswith("spark_target") for p in report.validate_perf(perf("baseline"), "spark-4.1", MANIFEST)))

    def test_nan_infinity_and_huge_values_are_rejected(self):
        with scratch_dir() as tmp:
            for literal in ("NaN", "Infinity", "-Infinity"):
                path = tmp / "r.json"
                path.write_text('{"makespan_ms": %s}' % literal, encoding="utf-8")
                with self.subTest(literal=literal), self.assertRaises(ValueError):
                    report.load_json(path)
        doc = perf("baseline")
        doc["views"][0]["wall_ms"] = 1e301
        self.assertTrue(any("implausibly large" in p for p in problems(doc)))
        doc = perf("baseline")
        doc["views"][0]["wall_ms"] = float("nan")
        self.assertTrue(any("not finite" in p for p in problems(doc)))

    def test_negative_and_non_numeric_values_are_rejected(self):
        doc = perf("baseline")
        doc["views"][1]["capture_wait_ms"] = -1
        doc["counts"]["state_sync"]["bytes_uploaded"] = "many"
        doc["views"][2]["publication_ms"] = True
        found = " ".join(problems(doc))
        self.assertIn("capture_wait_ms: must be >= 0", found)
        self.assertIn("bytes_uploaded: not a number", found)
        self.assertIn("publication_ms: not a number", found)

    def test_structure_errors(self):
        doc = perf("baseline")
        doc["views"].pop()
        self.assertTrue(any("exactly 6 entries" in p for p in problems(doc)))
        doc = perf("baseline")
        doc["views"][5]["mv_index"] = 1
        self.assertTrue(any("mv_index must be exactly 1..6" in p for p in problems(doc)))
        doc = perf("baseline")
        del doc["gates"]["parity"]
        self.assertTrue(any("gates.parity" in p for p in problems(doc)))

    def test_cross_field_consistency_cannot_be_forged(self):
        cases = {
            "must equal makespan_ms": lambda d: d.update(end_to_end_ms=d["makespan_ms"] / 2),
            "serial_sum_ms": lambda d: d.update(serial_sum_ms=1.0),
            "max_concurrent_engine": lambda d: d.update(max_concurrent_engine=1),
            "makespan_ms ends before the last durable ack": lambda d: d.update(makespan_ms=1.0, end_to_end_ms=1.0),
            "precedes": lambda d: d["views"][0].update(engine_end_at_ms=d["views"][0]["engine_start_at_ms"] - 1),
            "submit_to_ack_ms disagrees": lambda d: d["views"][0].update(submit_to_ack_ms=d["views"][0]["submit_to_ack_ms"] + 10_000),
        }
        for needle, mutate in cases.items():
            doc = perf("baseline")
            mutate(doc)
            with self.subTest(needle=needle):
                self.assertTrue(any(needle in p for p in problems(doc)), problems(doc))

    def test_baseline_reproduces_the_reference_evidence(self):
        doc = perf("baseline")
        by_mv = {v["mv_index"]: v for v in doc["views"]}
        self.assertEqual(by_mv[6]["capture_wait_ms"], 5509)
        self.assertEqual(by_mv[5]["catalog_finalize_ms"], 12439)
        self.assertEqual(by_mv[1]["publication_queue_wait_ms"], 5817)
        self.assertEqual(by_mv[6]["publication_ms"], 6119)
        self.assertEqual(doc["engine_overlap_ms"], 31809)
        self.assertEqual(report.engine_concurrency(doc), 6)


class OverlapTests(unittest.TestCase):
    def test_peak_concurrency_ignores_touching_and_empty_intervals(self):
        self.assertEqual(report.max_concurrent([(0, 10), (10, 20), (20, 30)]), 1)
        self.assertEqual(report.max_concurrent([(0, 10), (5, 15), (9, 12), (5, 5)]), 3)
        self.assertEqual(report.max_concurrent([]), 0)


class GateTests(unittest.TestCase):
    def test_healthy_report_has_no_gate_failures(self):
        self.assertEqual(report.perf_gate_failures(perf("good-candidate"), MANIFEST), {"correctness": [], "incremental": []})

    def test_parity_and_failed_outcome_fail_correctness(self):
        failures = report.perf_gate_failures(perf("bad-parity"), MANIFEST)
        self.assertIn("gates.parity is false", failures["correctness"][0])
        doc = perf("baseline")
        doc["views"][2]["outcome"] = "FAILED: timeout"
        self.assertIn("MV3 outcome", report.perf_gate_failures(doc, MANIFEST)["correctness"][0])

    def test_full_refresh_fails_the_incremental_gate_in_any_spelling(self):
        failures = report.perf_gate_failures(perf("full-refresh-regression"), MANIFEST)["incremental"]
        self.assertTrue(any("full_refresh_count=1" in f for f in failures))
        self.assertTrue(any("incremental_count=5" in f for f in failures))
        for spelling in ("full refresh", "Full-Refresh", " FULL_REFRESH "):
            with self.subTest(spelling=spelling):
                self.assertTrue(report.is_full_refresh(spelling))
        self.assertFalse(report.is_full_refresh("WINDOW_PARTITION"))


class CrashTests(unittest.TestCase):
    def test_good_report_passes_and_echoes_the_known_gap(self):
        doc = crash("baseline")
        self.assertEqual(report.validate_crash(doc, MANIFEST), [])
        failures, known = report.crash_gate(doc)
        self.assertEqual(failures, [])
        self.assertEqual(known, ["crash-between-mv-version-and-cdf-watermark"])

    def test_unrecovered_point_fails_but_known_gap_never_does(self):
        failures, _ = report.crash_gate(crash("bad-crash"))
        self.assertEqual(len(failures), 1)
        self.assertIn("recovered, metadata_converged false", failures[0])
        doc = crash("baseline")
        gap = next(p for p in doc["points"] if p["known_gap"])
        gap.update(recovered=False, parity=False)
        self.assertEqual(report.crash_gate(doc)[0], [])

    def test_schema_and_minimum_points(self):
        doc = crash("baseline")
        doc["points"] = doc["points"][:2]
        self.assertTrue(any("need at least 3" in p for p in report.validate_crash(doc, MANIFEST)))
        doc = copy.deepcopy(crash("baseline"))
        doc["schema"] = "wrong"
        doc["points"][0]["recovered"] = "yes"
        found = " ".join(report.validate_crash(doc, MANIFEST))
        self.assertIn("schema", found)
        self.assertIn("recovered: must be a boolean", found)


if __name__ == "__main__":
    unittest.main()
