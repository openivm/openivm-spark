#!/usr/bin/env python3
"""Measure combined driver/task observer cost on one binary; never rank a product change."""

import argparse
import collections
import fcntl
import statistics
import time
from pathlib import Path

import json
import paired
import scorecard


SOURCE = "0e5e330ec360b01ddf9f38e75f53ee976d3a7355"
DETAIL = {"spark.openivm.telemetry.deep.enabled": "true", "spark.openivm.telemetry.taskMetrics.enabled": "true"}


def physical_work(events, ids):
    """Count a physical attempt once, even when several primary requests reference its stage."""
    jobs = [e for e in events if e.get("event_type") == "spark_job_start" and e.get("request_id") in ids]
    owners = collections.defaultdict(set)
    for job in jobs:
        for stage in job["stage_ids"]:
            owners[job["clock_domain_id"], stage].add(job["request_id"])
    stages, statuses = {}, collections.Counter()
    for e in events:
        if e.get("event_type") != "spark_stage_completed":
            continue
        ref = (e["clock_domain_id"], e["stage_id"])
        if ref not in owners:
            continue
        key = (*ref, e["stage_attempt"])
        if key in stages:
            raise ValueError(f"duplicate stage completion {key}")
        stages[key] = e.get("task_summary")
        statuses[e.get("task_summary_status", "not-requested")] += 1
    available = [v for v in stages.values() if v is not None]
    totals = {k: sum(v[k] for v in available) if available else None
              for k in ("attempts", "attempts_with_metrics", "attempts_without_metrics")}
    if available and totals["attempts_with_metrics"] + totals["attempts_without_metrics"] != totals["attempts"]:
        raise ValueError("observed task count is not conserved")
    return {"primary_job_starts": len(jobs), "truncated_job_stage_lists": sum(j["stage_tracking_truncated"] for j in jobs),
            "physical_stage_attempts": len(stages), "shared_stage_attempts": sum(len(owners[k[:2]]) > 1 for k in stages),
            "referenced_stages_without_completion": len(set(owners) - {k[:2] for k in stages}),
            "summary_statuses": dict(statuses), "observed_task_counts": totals,
            "coverage": "observed-only; late callbacks after the last owner JobEnd are not observed"}


def read_lines(path):
    with path.open() as stream:
        return [json.loads(line) for line in stream if line.strip()]


def observation(report, directory):
    ids = set(report["primary"]["request_ids"])
    spans = [s for s in read_lines(directory / "r7-walkthrough-spans.jsonl") if s["request_id"] in ids]
    if len(spans) != 6 or {s["request_id"] for s in spans} != ids:
        raise ValueError("missing/duplicated primary completed span")
    event_path = directory / "r7-walkthrough-events.jsonl"
    events = read_lines(event_path)
    coverage = collections.Counter(s.get("deep_status", s.get("owner_thread_coverage_status", "unavailable-or-off"))
                                   for s in spans)
    return {"durable_ms": report["primary"]["durable_ms"], "return_ms": report["primary"]["return_ms"],
            "queue_delta": report["primary"]["queue_delta"], "fs_ops": report["primary"]["fs_ops"],
            "walkthrough_event_bytes": event_path.stat().st_size, "walkthrough_event_records": len(events),
            "driver_coverage": dict(coverage), "spark": physical_work(events, ids),
            "summed_driver_observer_ns_lower_bound": sum(s["deep_observer_ns"] for s in spans)
            if all("deep_observer_ns" in s for s in spans) else None}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    root = paired.repo_root()
    c = scorecard.load_contract(paired.CONTRACT)
    sha = paired.git(root, "rev-parse", "HEAD")
    digest, _ = paired.harness_digest(root, c["harness_paths"])
    paired.need(digest == c["_harness_digest"] and not paired.git(root, "status", "--porcelain"), "unclean contract")
    paired.need(Path("/.dockerenv").exists(), "run inside the pinned devcontainer")
    host = paired.host_identity(c)
    native = paired.native_identity(c)
    paired.need(native == c["identities"]["native"], "native identity mismatch")
    out = args.out.resolve()
    paired.need(not out.is_relative_to(root), "use a fresh directory outside the contract tree")
    order = [[n, {"control": "legacy", "candidate": "deep"}[a]] for n, a in scorecard.frozen_order(3)]
    plan = {"series_id": out.name, "comparison_class": "observer-cost", "source": SOURCE, "contract_sha": sha,
            "harness_digest": digest, "contract_digest": c["_digest"], "order": order, "target": "spark-4.1",
            "workload": "primary", "native": native, "profiles": {"legacy": {}, "deep": DETAIL},
            "host": host,
            "frozen_unix_s": time.time(), "exclusions": c["exclusions"]}
    with open("/tmp/openivm-r7-series.lock", "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        out.mkdir(parents=True, exist_ok=False)
        paired.write(out / "plan.json", plan)
        wt = Path(f"{out}.src")
        paired.prepare(root, sha, SOURCE, wt, c["harness_paths"])
        outcomes, problems = {}, []
        for n, arm in order:
            key = f"p{n:02d}-{arm}"
            if problems:
                paired.write(out / f"{key}.not-run.json", {"reason": problems})
                continue
            profile = {"telemetry_conf": plan["profiles"][arm]}
            arm_contract = dict(c, profiles={arm: profile})
            evidence = paired.run_arm(arm_contract, dict(plan, profile=arm), n, arm, wt, out, 3600)
            if evidence["status"] != "completed" or not evidence["report"]:
                problems.append(f"{key}: {evidence['status']} (exit={evidence['exit_code']})")
                continue
            report = evidence["report"]
            scorecard.check_primary(c, profile, report, problems, key)
            if report["runtime"] != c["expected"]["runtime"]["spark-4.1"]:
                problems.append(f"{key}: wrong runtime")
            outcomes[key] = observation(report, out / key)
            print(f"{key}: {outcomes[key]['durable_ms']} ms; problems={problems}", flush=True)
        result = {"comparison_class": "observer-cost", "shortlisted": False, "release_acceptance": False,
                  "valid": not problems, "problems": problems, "arms": outcomes}
        if not problems:
            diffs = [100 * (outcomes[f"p{n:02d}-deep"]["durable_ms"] / outcomes[f"p{n:02d}-legacy"]["durable_ms"] - 1)
                     for n in range(1, 4)]
            result.update(deep_cost_percent_by_pair=diffs, median_deep_cost_percent=statistics.median(diffs))
        paired.write(out / "observer-cost.json", result)
        if problems:
            raise SystemExit("observer series invalid: " + "; ".join(problems))


if __name__ == "__main__":
    main()
