"""Discriminator tests for the R7 scorecard: invalid evidence never earns speed credit."""

import copy
import io
import json
import math
import unittest
from contextlib import redirect_stdout
from pathlib import Path

import paired
import scorecard
import observer

HERE = Path(__file__).resolve().parent
CANDIDATE = "1" * 40


def contract():
    c = json.loads((HERE / "contract.json").read_text())
    c["_digest"], c["_harness_digest"] = "c" * 64, "h" * 64
    return c


def primary_report(c, sha, label, durable, ret, tag):
    exp = c["expected"]
    views = [f"openivm_demo.concurrent_{k}" for k in range(1, 7)]
    ids = [f"req_{tag}_{k}" for k in range(6)]
    source = "openivm_demo.parallel_events"
    queue = dict.fromkeys(("queued", "dropped_closed", "dropped_full", "dropped_contended", "dropped_oversized",
                           "worker_failures", "manifest_failures", "export_failures"), 0)
    queue.update(accepted=12, published=12)
    requests = [{"view": v, "request_id": i, "operation": "refresh", "outcome": "incremental_executed",
                 "effective_refresh_type": "AGGREGATE_GROUP", "errors": [], "capture_status": "complete",
                 "drain_snapshot_completed": True, "return_at_ms": ret, "durable_at_ms": durable,
                 "source_versions": json.dumps([{"relation": source, "start_version": 0, "end_version": 1}]),
                 "state_barrier_wait_ms": 1.0, "state_barrier_hold_ms": 2.0, "catalog_finalize_ms": 30.0,
                 "engine_exec_ms": 40.0, "command_return_ms": ret, "request_durable_ms": durable,
                 "insights_after_return_ms": 5.0, "drain_ms": 25.0} for v, i in zip(views, ids)]
    return {"schema": "openivm.r7-walkthrough-perf/v1", "workload": c["workloads"]["primary"]["workload_id"],
            "role": "primary", "label": label, "head_sha": sha, "spark_target": "spark-4.1", "spark_version": "4.1.0",
            "capture_pins": dict(exp["capture_pins"]), "conf": dict(exp["primary_conf"]),
            "latency_profile": dict(exp["latency_profile"]), "insights_poll_ms": exp["insights_poll_ms"],
            "telemetry_conf": {}, "runtime": exp["runtime"]["spark-4.1"].copy(), "master": "local[8]",
            "mode_proof": {"conf_mode": "cdf", "feature_gate_mode": "cdf", "source_cdf_property": "true",
                           "source": source, "source_id_before": tag, "source_id_after": tag,
                           "source_version_before": 0, "source_version_after": 1,
                           "cdf_watermarks": {v: {"openivm_demo.parallel_events": 1} for v in views}},
            "gates": {g: True for g in exp["required_gates"]},
            "primary": {"workers": 6, "request_ids": ids, "incremental_count": 6, "queue_delta": queue,
                        "fs_ops": {"catalog.read": 10, "state_sync.write": 4},
                        "full_refresh_count": 0, "durable_ms": durable, "return_ms": ret},
            "batches": {"parallel_refresh": {"requests": requests, "workers": 6, "max_concurrent_commands": 6,
                                            "request_ids": ids[:], "durable_ms": durable, "return_ms": ret}}, "failures": []}


def series(control, candidate, returns=None):
    c = contract()
    pairs = len(control)
    plan = {"schema": "openivm.r7-series/v1", "series_id": "t", "comparison_class": "code-ab", "workload": "primary",
            "profile": "default", "target": "spark-4.1", "pairs": pairs, "order": scorecard.frozen_order(pairs),
            "control": {"sha": c["profiles"]["default"]["control_sha"]}, "candidate": {"sha": CANDIDATE},
            "contract_digest": c["_digest"], "harness_digest": c["_harness_digest"],
            "native": dict(c["identities"]["native"]), "exclusions": dict(c["exclusions"]),
            "contract_sha": "b" * 40, "host": {"node": "host", "cpus": 16, "image": "sha256:" + "1" * 64},
            "frozen_unix_s": 1}
    arms = []
    for sequence, (pair, arm) in enumerate(plan["order"], 1):
        durable = (control if arm == "control" else candidate)[pair - 1]
        ret = returns[arm][pair - 1] if returns else durable * 0.75
        sha, label = plan[arm]["sha"], f"t-p{pair:02d}-{arm}"
        arms.append({"pair": pair, "arm": arm, "sha": sha, "label": label, "status": "completed", "exit_code": 0,
                     "host": plan["host"].copy(), "started_unix_s": sequence * 10, "ended_unix_s": sequence * 10 + 1,
                     "harness_digest": plan["harness_digest"], "native": dict(plan["native"]),
                     "report": primary_report(c, sha, label, durable, ret, f"{pair}{arm}")})
    return c, plan, arms


def arm(arms, pair, name):
    return next(a for a in arms if a["pair"] == pair and a["arm"] == name)


class ScorecardTest(unittest.TestCase):
    def assertInvalid(self, result):
        self.assertFalse(result["valid"])
        self.assertFalse(result["shortlisted"])
        self.assertNotIn("median_reduction_percent", result)

    def test_valid_series_shortlists_without_release_acceptance(self):
        result = scorecard.score(*series([1000, 1000, 1000], [900, 880, 910]))
        self.assertTrue(result["valid"] and result["shortlisted"])
        self.assertEqual(result["median_reduction_percent"], 10.0)
        self.assertFalse(result["release_acceptance"])
        self.assertEqual(result["native_decision"], "not-evaluated-by-r7-scorecard")

    def test_threshold_is_inclusive_without_rounding(self):
        self.assertTrue(scorecard.score(*series([100] * 3, [97] * 3))["shortlisted"])
        below = scorecard.score(*series([100000] * 3, [97001] * 3))
        self.assertTrue(below["valid"])
        self.assertFalse(below["shortlisted"])

    def test_every_pair_must_improve(self):
        result = scorecard.score(*series([100, 100, 100], [80, 80, 100]))
        self.assertTrue(result["valid"])
        self.assertFalse(result["shortlisted"])

    def test_plan_discriminators(self):
        cases = {
            "fewer than three pairs": (2, lambda p: None),
            "wrong order": (3, lambda p: p.update(order=[p["order"][1], p["order"][0], *p["order"][2:]])),
            "cross control": (3, lambda p: p["control"].update(sha="2" * 40)),
            "other contract": (3, lambda p: p.update(contract_digest="d" * 64)),
            "native changed": (3, lambda p: p["native"].update(cli_sha256="0" * 64)),
            "excluded after freeze": (3, lambda p: p["exclusions"].update(post_hoc="drop slow arm")),
            "observer-cost class": (3, lambda p: p.update(comparison_class="observer-cost")),
            "primary under companion": (3, lambda p: p.update(workload="companion")),
        }
        for name, (pairs, mutate) in cases.items():
            with self.subTest(name):
                c, p, a = series([100] * pairs, [80] * pairs)
                mutate(p)
                self.assertInvalid(scorecard.score(c, p, a))

    def test_arm_discriminators(self):
        def report(path, value):
            def apply(x):
                *parents, key = path.split("/")
                target = x["report"]
                for part in parents:
                    target = target[part]
                target[key] = value
            return apply

        cases = {
            "failed arm": lambda x: x.update(status="failed", exit_code=1),
            "harness bytes changed": lambda x: x.update(harness_digest="e" * 64),
            "different commit": lambda x: x.update(sha="3" * 40),
            "out-of-order attempt": lambda x: x.update(started_unix_s=1),
            "host changed": lambda x: x["host"].update(node="other"),
            "missing report": lambda x: x.update(report=None),
            "telemetry pins differ": report("telemetry_conf", {"spark.openivm.telemetry.deep.enabled": "true"}),
            "intercept mode": report("mode_proof/conf_mode", "intercept"),
            "source CDF off": report("mode_proof/source_cdf_property", "false"),
            "source replaced": report("mode_proof/source_id_after", "other"),
            "false six aggregate": lambda x: x["report"]["batches"]["parallel_refresh"]["requests"][0].update(
                outcome="full_refresh_executed"),
            "unpublished": report("primary/queue_delta/published", 11),
            "no classloader proof": report("runtime/delta_jar", ""),
            "watermark behind": report("mode_proof/cdf_watermarks/openivm_demo.concurrent_1",
                                       {"openivm_demo.parallel_events": 0}),
            "full refresh": report("primary/full_refresh_count", 1),
            "parity gate failed": report("gates/parity", False),
            "gate missing": lambda x: x["report"]["gates"].pop("state_sync_committed"),
            "conf drift": report("conf/spark.sql.shuffle.partitions", "8"),
            "latency drift": report("latency_profile/scale", 0.5),
            "null durable": report("primary/durable_ms", None),
            "nonfinite durable": report("primary/durable_ms", math.nan),
            "return after durable": report("primary/return_ms", 10_000),
            "five requests": report("primary/request_ids", ["a", "b", "c", "d", "e"]),
            "wrong runtime": report("spark_version", "3.5.1"),
            "recorded failure": report("failures", ["boom"]),
        }
        for name, mutate in cases.items():
            with self.subTest(name):
                c, p, a = series([100] * 3, [80] * 3)
                mutate(arm(a, 2, "candidate"))
                self.assertInvalid(scorecard.score(c, p, a))

    def test_series_discriminators(self):
        c, p, a = series([100] * 3, [80] * 3)
        arm(a, 3, "candidate")["report"]["primary"]["request_ids"] = arm(a, 1, "control")["report"]["primary"][
            "request_ids"]
        self.assertInvalid(scorecard.score(c, p, a))
        c, p, a = series([100] * 3, [80] * 3)
        self.assertInvalid(scorecard.score(c, p, a[:-1]))
        c, p, a = series([100] * 3, [80] * 3)
        self.assertInvalid(scorecard.score(c, p, a + [copy.deepcopy(a[0])]))

    def test_early_return_only_earns_no_credit(self):
        returns = {"control": [90] * 3, "candidate": [50] * 3}
        result = scorecard.score(*series([100] * 3, [100] * 3, returns))
        self.assertTrue(result["valid"])
        self.assertFalse(result["shortlisted"])
        self.assertTrue(result["early_return_only"])

    def test_native_decision_is_passed_through(self):
        native = {"accepted": False, "evaluator": "742eac69"}
        result = scorecard.score(*series([100] * 3, [50] * 3), native_decision=native)
        self.assertTrue(result["shortlisted"])
        self.assertEqual(result["native_decision"], native)
        self.assertFalse(result["release_acceptance"])

    def test_missing_phase_is_unavailable_not_zero(self):
        c, p, a = series([100] * 3, [80] * 3)
        rows = arm(a, 1, "candidate")["report"]["batches"]["parallel_refresh"]["requests"]
        del rows[0]["catalog_finalize_ms"]
        for row in rows:
            row["state_barrier_wait_ms"] = None
        phases = scorecard.score(c, p, a)["pairs"][0]["phases"]["candidate"]
        self.assertEqual(phases["catalog_finalize_ms"], {"median": None, "coverage": "partial"})
        self.assertEqual(phases["state_barrier_wait_ms"], {"median": None, "coverage": "unavailable"})

    def test_invalid_emit_is_finite_and_explains(self):
        c, p, a = series([100] * 3, [80] * 3)
        arm(a, 1, "control").update(status="failed", exit_code=1)
        out = io.StringIO()
        with redirect_stdout(out):
            scorecard.emit(scorecard.score(c, p, a))
        lines = out.getvalue().splitlines()
        self.assertEqual(lines[0], "@RESULT valid=0 shortlisted=0 median_reduction_percent=0.000000")
        self.assertTrue(lines[1].startswith("@FEEDBACK invalid series, no speed credited: "))

    def test_lock_matches_harness_files(self):
        lock = json.loads((HERE / "contract.lock.json").read_text())
        digest, listing = paired.harness_digest(paired.repo_root(), contract()["harness_paths"])
        self.assertEqual(listing, lock["files"])
        self.assertEqual(digest, lock["harness_digest"])

    def test_shared_stage_work_is_counted_once_and_partial_metrics_remain_partial(self):
        jobs = [{"event_type": "spark_job_start", "request_id": i, "clock_domain_id": "d", "stage_ids": [1, 2],
                 "stage_tracking_truncated": False} for i in ("a", "b")]
        stage = {"event_type": "spark_stage_completed", "clock_domain_id": "d", "stage_id": 1, "stage_attempt": 0,
                 "task_summary_status": "observed", "task_summary": {"attempts": 3, "attempts_with_metrics": 2,
                                                                    "attempts_without_metrics": 1}}
        got = observer.physical_work([*jobs, stage], {"a", "b"})
        self.assertEqual(got["physical_stage_attempts"], 1)
        self.assertEqual(got["shared_stage_attempts"], 1)
        self.assertEqual(got["referenced_stages_without_completion"], 1)
        self.assertEqual(got["observed_task_counts"]["attempts_without_metrics"], 1)
        with self.assertRaisesRegex(ValueError, "duplicate stage"):
            observer.physical_work([*jobs, stage, stage], {"a", "b"})


if __name__ == "__main__":
    unittest.main()
