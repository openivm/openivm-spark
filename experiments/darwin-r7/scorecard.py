#!/usr/bin/env python3
"""Pure R7 round scorecard: validates one frozen paired series and computes the shortlist decision.

It reads no clock or environment, so the same inputs always score the same. The scorecard never accepts a candidate
for release and never alters a native evaluator decision.
"""

import hashlib
import json
import math
import re
import statistics
import sys
from pathlib import Path

SHA = re.compile(r"[0-9a-f]{40}")
SPARK_VERSIONS = {"spark-3.5": "3.5.1", "spark-4.1": "4.1.0"}
ARMS = ("control", "candidate")


def load_contract(path):
    data = Path(path).read_bytes()
    contract = json.loads(data)
    lock = json.loads((Path(path).parent / "contract.lock.json").read_text())
    contract["_digest"] = hashlib.sha256(data).hexdigest()
    contract["_harness_digest"] = lock["harness_digest"]
    return contract


def need(ok, reason, problems):
    if not ok:
        problems.append(reason)
    return ok


def finite_positive(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value > 0


def frozen_order(pairs):
    """AB, BA, AB, ... with the control first in odd pairs."""
    return [[p, arm] for p in range(1, pairs + 1) for arm in (ARMS if p % 2 else ARMS[::-1])]


def dig(obj, dotted):
    for key in dotted.split("."):
        if not isinstance(obj, dict) or key not in obj:
            return None
        obj = obj[key]
    return obj


def check_plan(contract, plan, problems):
    workload = contract["workloads"].get(plan.get("workload"))
    profile = contract["profiles"].get(plan.get("profile"))
    need(plan.get("schema") == "openivm.r7-series/v1", "plan schema mismatch", problems)
    need(plan.get("comparison_class") == "code-ab", "only the code-ab comparison class is scored here", problems)
    need(workload is not None, f"unknown workload {plan.get('workload')!r}", problems)
    need(profile is not None, f"unknown profile {plan.get('profile')!r}", problems)
    need(plan.get("target") in SPARK_VERSIONS, f"unknown target {plan.get('target')!r}", problems)
    need(plan.get("contract_digest") == contract["_digest"], "series was not run under this contract digest",
         problems)
    need(plan.get("harness_digest") == contract["_harness_digest"], "harness digest differs from the contract lock",
         problems)
    pairs = plan.get("pairs")
    minimum = contract["shortlist"]["min_pairs"]
    need(isinstance(pairs, int) and not isinstance(pairs, bool) and pairs >= minimum,
         f"fewer than {minimum} pairs planned", problems)
    if isinstance(pairs, int):
        need(plan.get("order") == frozen_order(pairs), "order is not the frozen alternating AB/BA order", problems)
    shas = {arm: dig(plan, f"{arm}.sha") for arm in ARMS}
    for arm, sha in shas.items():
        need(isinstance(sha, str) and SHA.fullmatch(sha) is not None, f"{arm} sha is not a full commit", problems)
    need(shas["control"] != shas["candidate"], "control and candidate are the same commit", problems)
    if profile is not None:
        need(profile.get("control_sha") is not None, f"profile {plan.get('profile')} has no frozen control", problems)
        need(shas["control"] == profile.get("control_sha"), "cross-control pairing: control is not the profile control",
             problems)
    need(plan.get("native") == contract["identities"]["native"], "native identity differs from the contract",
         problems)
    need(plan.get("exclusions") == contract["exclusions"], "exclusion policy changed", problems)
    host = plan.get("host") or {}
    need(isinstance(host.get("node"), str) and host["node"] and type(host.get("cpus")) is int and host["cpus"] > 0
         and re.fullmatch(r"sha256:[0-9a-f]{64}", str(host.get("image"))), "missing host/image identity", problems)
    need(SHA.fullmatch(str(plan.get("contract_sha"))) is not None, "missing contract commit", problems)
    return workload, profile


def check_primary(contract, profile, report, problems, where):
    exp = contract["expected"]
    need(report.get("schema") == "openivm.r7-walkthrough-perf/v1", f"{where}: schema", problems)
    need(report.get("workload") == contract["workloads"]["primary"]["workload_id"], f"{where}: workload", problems)
    need(report.get("role") == "primary", f"{where}: role", problems)
    need(report.get("capture_pins") == exp["capture_pins"], f"{where}: capture pins", problems)
    need(report.get("conf") == exp["primary_conf"], f"{where}: effective configuration", problems)
    need(report.get("latency_profile") == exp["latency_profile"], f"{where}: latency profile", problems)
    need(report.get("insights_poll_ms") == exp["insights_poll_ms"], f"{where}: INSIGHTS poll interval", problems)
    need(report.get("telemetry_conf") == profile["telemetry_conf"], f"{where}: telemetry pins", problems)
    mode = report.get("mode_proof") or {}
    need(mode.get("conf_mode") == "cdf" and mode.get("feature_gate_mode") == "cdf", f"{where}: effective mode not cdf",
         problems)
    need(mode.get("source_cdf_property") == "true", f"{where}: source CDF disabled", problems)
    before, after = mode.get("source_version_before"), mode.get("source_version_after")
    source = "openivm_demo.parallel_events"
    views = {f"openivm_demo.concurrent_{i}" for i in range(1, 7)}
    need(mode.get("source") == source and mode.get("source_id_before") and
         mode["source_id_before"] == mode.get("source_id_after") and (before, after) == (0, 1),
         f"{where}: physical source identity/version interval", problems)
    marks = mode.get("cdf_watermarks") or {}
    need(set(marks) == views and all(m == {source: after} for m in marks.values()),
         f"{where}: CDF watermarks did not advance to the inserted version", problems)
    gates = report.get("gates") or {}
    missing = [g for g in exp["required_gates"] if gates.get(g) is not True]
    need(not missing, f"{where}: failed or missing gates {missing}", problems)
    primary = report.get("primary") or {}
    ids = primary.get("request_ids") or []
    need(primary.get("workers") == 6 and len(ids) == 6 and all(isinstance(s, str) and s for s in ids)
         and len(set(ids)) == 6, f"{where}: not six unique requests",
         problems)
    need(primary.get("incremental_count") == 6 and primary.get("full_refresh_count") == 0,
         f"{where}: not 6 incremental / 0 full", problems)
    durable, ret = primary.get("durable_ms"), primary.get("return_ms")
    need(finite_positive(durable), f"{where}: durable_ms null, nonfinite or nonpositive", problems)
    need(finite_positive(ret), f"{where}: return_ms null, nonfinite or nonpositive", problems)
    if finite_positive(durable) and finite_positive(ret):
        need(ret <= durable, f"{where}: command return after durable completion", problems)
    batch = dig(report, "batches.parallel_refresh") or {}
    rows = batch.get("requests") or []
    need(len(rows) == 6 and set(ids) == {r.get("request_id") for r in rows} and
         views == {r.get("view") for r in rows} and batch.get("max_concurrent_commands") == 6 and
         batch.get("workers") == 6 and batch.get("request_ids") == ids, f"{where}: batch identity/overlap", problems)
    for r in rows:
        expected_versions = [{"relation": source, "start_version": before, "end_version": after}]
        try:
            versions = json.loads(r.get("source_versions", "null"))
        except (ValueError, TypeError):
            versions = None
        need(versions == expected_versions and r.get("outcome") == "incremental_executed" and
             r.get("effective_refresh_type") == "AGGREGATE_GROUP" and r.get("operation") == "refresh" and
             r.get("capture_status") == "complete" and r.get("drain_snapshot_completed") is True and
             r.get("errors") == [], f"{where}: request {r.get('request_id')} evidence", problems)
    for key, value, end in (("durable_ms", durable, "durable_at_ms"), ("return_ms", ret, "return_at_ms")):
        ends = [r.get(end) for r in rows]
        need(batch.get(key) == value and ends and all(finite_positive(x) for x in ends) and max(ends) == value,
             f"{where}: {key} not bound to last request", problems)
    queue = primary.get("queue_delta") or {}
    need(finite_positive(queue.get("accepted")) and queue.get("published") == queue.get("accepted") and
         all(queue.get(k) == 0 for k in ("queued", "dropped_closed", "dropped_full", "dropped_contended",
                                        "dropped_oversized", "worker_failures", "manifest_failures", "export_failures")),
         f"{where}: incomplete batch queue/export", problems)
    need(not report.get("failures"), f"{where}: recorded failures", problems)
    return ids, durable, ret


def check_companion(contract, profile, report, problems, where):
    need(report.get("schema") == "openivm.r7-matched-intercept/v1", f"{where}: schema", problems)
    need(report.get("workload") == contract["workloads"]["companion"]["workload_id"], f"{where}: workload", problems)
    need(report.get("role") == "companion", f"{where}: role", problems)
    need(report.get("telemetry_conf") == profile["telemetry_conf"], f"{where}: telemetry pins", problems)
    need((report.get("mode"), report.get("catalog_backend"), report.get("async"), report.get("source_cdf_enabled")) ==
         ("intercept", "delta", "false", True), f"{where}: propagation/catalog/sync configuration", problems)
    need((report.get("seed_rows"), report.get("insert_rows"), report.get("final_rows")) == (3, 3, 6),
         f"{where}: input shape", problems)
    need(report.get("latency") == contract["expected"]["companion_latency"], f"{where}: latency profile", problems)
    views = report.get("views") or []
    ids = [v.get("request_id") for v in views]
    need(len(views) == 6 and len(set(ids)) == 6 and report.get("requests_peak") == 6,
         f"{where}: not six concurrent unique requests", problems)
    for v in views:
        need((v.get("outcome"), v.get("effective_refresh_type"), v.get("pending_after"),
              v.get("bidirectional_except_all")) == ("incremental_executed", "AGGREGATE_GROUP", 0, True),
             f"{where}: view {v.get('view')} correctness", problems)
    makespan = report.get("makespan_ms")
    need(finite_positive(makespan), f"{where}: makespan_ms null, nonfinite or nonpositive", problems)
    return ids, makespan, None


PHASES = ("state_barrier_wait_ms", "state_barrier_hold_ms", "catalog_finalize_ms", "engine_exec_ms",
          "command_return_ms", "request_durable_ms", "insights_after_return_ms", "drain_ms")


def phase_medians(report):
    """Diagnostic medians of nested, non-additive phases; a phase missing on any request is not a measured zero."""
    rows = dig(report, "batches.parallel_refresh.requests") or []
    out = {}
    for key in PHASES:
        measured = [r.get(key) for r in rows if isinstance(r.get(key), (int, float)) and math.isfinite(r.get(key))]
        complete = bool(rows) and len(measured) == len(rows)
        out[key] = {"median": statistics.median(measured) if complete else None,
                    "coverage": "measured" if complete else ("partial" if measured else "unavailable")}
    return out


def reduction(control, candidate):
    return 100.0 * (control - candidate) / control


def score(contract, plan, arms, native_decision=None):
    problems = []
    workload, profile = check_plan(contract, plan, problems)
    order = plan.get("order") if isinstance(plan.get("order"), list) else []
    expected = {tuple(row) for row in order if isinstance(row, list) and len(row) == 2
                and type(row[0]) is int and row[1] in ARMS}
    seen, rows, ids_all = set(), {}, []
    for arm in arms:
        key = (arm.get("pair"), arm.get("arm"))
        where = f"pair {key[0]} {key[1]}"
        if not need(key in expected, f"{where}: not in the frozen order", problems):
            continue
        if not need(key not in seen, f"{where}: duplicate arm", problems):
            continue
        seen.add(key)
        need(arm.get("host") == plan.get("host"), f"{where}: host/image drift", problems)
        label = f"{plan.get('series_id')}-p{key[0]:02d}-{key[1]}"
        need(arm.get("label") == label, f"{where}: not the planned unique attempt label", problems)
        need(arm.get("status") == "completed" and arm.get("exit_code") == 0,
             f"{where}: {arm.get('status')} (exit {arm.get('exit_code')}) is never scored", problems)
        need(arm.get("sha") == dig(plan, f"{key[1]}.sha"), f"{where}: ran a different commit", problems)
        need(arm.get("harness_digest") == plan.get("harness_digest"), f"{where}: harness bytes changed", problems)
        need(arm.get("native") == plan.get("native"), f"{where}: native identity changed", problems)
        report = arm.get("report")
        if not need(isinstance(report, dict), f"{where}: missing report", problems):
            continue
        if workload is None or profile is None:
            continue
        need(report.get("head_sha", report.get("head")) == arm.get("sha"), f"{where}: report head mismatch", problems)
        need(report.get("label") == arm.get("label"), f"{where}: report label mismatch", problems)
        target = report.get("spark_target", report.get("target"))
        need(target == plan.get("target") and report.get("spark_version") == SPARK_VERSIONS.get(plan.get("target")),
             f"{where}: runtime mismatch", problems)
        need(report.get("runtime") == contract["expected"]["runtime"].get(target) and report.get("master") == "local[8]",
             f"{where}: loaded dependencies or master mismatch", problems)
        check = check_primary if plan["workload"] == "primary" else check_companion
        ids, scored, ret = check(contract, profile, report, problems, where)
        ids_all.extend(ids)
        rows[key] = {"scored_ms": scored, "return_ms": ret, "start": arm.get("started_unix_s"),
                     "end": arm.get("ended_unix_s"), "fs_ops": dig(report, "primary.fs_ops") or report.get("fs_ops"),
                     "phases": phase_medians(report) if plan["workload"] == "primary" else None}
    need(seen == expected, f"missing arms {sorted(expected - seen)}", problems)
    need(len(ids_all) == len(set(ids_all)), "request ids repeat across arms", problems)
    prior = plan.get("frozen_unix_s")
    if not finite_positive(prior):
        problems.append("missing plan freeze time")
    elif not problems:
        for pair, side in order:
            r = rows[pair, side]
            if not (finite_positive(r["start"]) and finite_positive(r["end"]) and prior <= r["start"] < r["end"]):
                problems.append(f"pair {pair} {side}: missing, overlapping or out-of-order attempt")
                break
            prior = r["end"]

    result = {"schema": "openivm.r7-scorecard/v1", "workload": plan.get("workload"),
              "role_label": workload["label"] if workload else None,
              "comparison_class": plan.get("comparison_class"), "profile": plan.get("profile"),
              "target": plan.get("target"), "control": dig(plan, "control.sha"),
              "candidate": dig(plan, "candidate.sha"), "contract_digest": contract["_digest"],
              "native_decision": native_decision if native_decision is not None else "not-evaluated-by-r7-scorecard",
              "release_acceptance": False}
    if problems:
        result.update(valid=False, shortlisted=False, problems=problems)
        return result

    pairs = []
    for p in range(1, plan["pairs"] + 1):
        c, k = rows[p, "control"], rows[p, "candidate"]
        entry = {"pair": p, "control_ms": c["scored_ms"], "candidate_ms": k["scored_ms"],
                 "reduction_percent": reduction(c["scored_ms"], k["scored_ms"]),
                 "filesystem_operations": {"control": c["fs_ops"], "candidate": k["fs_ops"]}}
        if c["return_ms"] is not None:
            entry["return_reduction_percent"] = reduction(c["return_ms"], k["return_ms"])
            entry["phases"] = {"control": c["phases"], "candidate": k["phases"]}
        pairs.append(entry)
    threshold = contract["shortlist"]["min_median_reduction_percent"]
    reductions = [e["reduction_percent"] for e in pairs]
    median = statistics.median(reductions)
    shortlisted = plan["workload"] == "primary" and min(reductions) > 0 and median >= threshold
    result.update(valid=True, shortlisted=shortlisted, pairs=pairs, median_reduction_percent=median,
                  min_reduction_percent=min(reductions), max_reduction_percent=max(reductions))
    if "return_reduction_percent" in pairs[0]:
        ret_median = statistics.median(e["return_reduction_percent"] for e in pairs)
        result["median_return_reduction_percent"] = ret_median
        result["early_return_only"] = (not shortlisted) and ret_median >= threshold
    return result


def emit(result):
    valid = 1 if result["valid"] else 0
    short = 1 if result["shortlisted"] else 0
    median = result.get("median_reduction_percent", 0.0)
    print(f"@RESULT valid={valid} shortlisted={short} median_reduction_percent={median:.6f}")
    if not valid:
        print("@FEEDBACK invalid series, no speed credited: " + "; ".join(result["problems"][:10]))
    elif result.get("early_return_only"):
        print("@FEEDBACK command return improved but durable completion did not; early return earns no credit")
    else:
        print(f"@FEEDBACK per-pair reductions {[round(p['reduction_percent'], 3) for p in result['pairs']]}")


def main(argv):
    if len(argv) != 3:
        print("usage: scorecard.py <contract.json> <series-dir>", file=sys.stderr)
        return 2
    contract = load_contract(argv[1])
    series = Path(argv[2])
    plan = json.loads((series / "plan.json").read_text())
    arms = []
    for p in sorted(series.glob("*/arm.json")):
        arm = json.loads(p.read_text())
        raw = arm.get("evidence_sha256") or {}
        role = plan.get("workload")
        report_name = contract["workloads"].get(role, {}).get("report")
        required = ({"nx.log", report_name, "r7-walkthrough-events.jsonl", "r7-walkthrough-spans.jsonl",
                     "r7-walkthrough-receipts.jsonl"} if role == "primary" else
                    {"nx.log", report_name, "r7-matched-intercept-events.jsonl", "r7-matched-intercept-receipts.jsonl"})
        if arm.get("status") == "completed":
            def matches(name):
                return isinstance(name, str) and Path(name).name == name and (p.parent / name).is_file() and \
                    hashlib.sha256((p.parent / name).read_bytes()).hexdigest() == raw[name]
            if not required.issubset(raw) or not all(matches(name) for name in raw) or \
                    arm.get("report") != json.loads((p.parent / report_name).read_text()):
                arm["status"] = "invalid-original-evidence"
        arms.append(arm)
    result = score(contract, plan, arms, plan.get("native_decision"))
    (series / "scorecard.json").write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    emit(result)
    return 0 if result["valid"] else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
