"""Read-only analysis of a canonical R7 walkthrough control; no scores or product changes."""
import argparse
import collections
import hashlib
import json
import pathlib
import statistics
import tarfile


def union(intervals):
    end = None
    result = 0
    for a, b in sorted(intervals):
        if end is None or a >= end:
            result += b - a
            end = b
        elif b > end:
            result += b - end
            end = b
    return result


def ms(n):
    return round(n / 1e6, 3)


def pair_bounds(a, b):
    return [ms(a), ms(b)]


def analyze(data, raw, batch):
    report = json.loads(data)
    workload = report["workload"]
    allowed = {"primary:cdf-walkthrough-six-mv-insert": ("primary", "openivm.r7-walkthrough-perf/v1"),
               "diagnostic:cdf-walkthrough-worker-order": ("supplemental", "openivm.r7-worker-order/v1")}
    if allowed.get(workload) != (report["role"], report["schema"]) or not all(report["gates"].values()):
        raise ValueError(f"{report['label']}: unrecognized or failing CDF run {report['gates']}")
    b = report["batches"][batch]
    request = {r["request_id"]: r for r in b["requests"]}
    events = [e for line in raw.splitlines() if (e := json.loads(line)).get("request_id") in request]
    phases = [e for e in events if e.get("event_type") == "phase"]
    domains = {e["clock_domain_id"] for e in phases}
    if len(domains) != 1 or len(request) != 6:
        raise ValueError("Phase intervals must be complete and in one driver clock domain")
    by_name = collections.defaultdict(list)
    by_request = collections.defaultdict(list)
    for e in phases:
        start = e["start_monotonic_ns"]
        end = start + e["inclusive_ns"]
        if e["outcome"] != "success" or end < start:
            raise ValueError(f"Invalid phase {e}")
        by_name[e["phase"]].append((start, end))
        by_request[e["request_id"]].append((start, end))
    for rid in request:
        if not any(e["request_id"] == rid and e["phase"] == "execute_refresh_sql_stmt" for e in phases):
            raise ValueError("Missing query's last statement")
    stmts = by_name["execute_refresh_sql_stmt"]
    last_sql = max(b for _, b in stmts)
    last_phase = max(b for iv in by_request.values() for _, b in iv)

    # The unmodified fixture stores request-relative bridge boundaries, not the batch's
    # absolute origin. Every synchronous REFRESH phase is enclosed by that request's
    # bridge call. Intersect all six constraints; retain 1 us for printed-ms rounding.
    low = max(max(b for _, b in by_request[rid]) - round(r["return_at_ms"] * 1e6) - 1000
              for rid, r in request.items())
    high = min(min(a for a, _ in by_request[rid]) - round(r["bridge_start_at_ms"] * 1e6) + 1000
               for rid, r in request.items())
    if high < low:
        raise ValueError("No compatible monotonic origin")
    exact_origin = b.get("batch_start_monotonic_ns")
    if exact_origin is not None and not low <= exact_origin <= high:
        raise ValueError("Recorded batch clock is incompatible with phase/bridge enclosures")
    last_return = round(b["return_ms"] * 1e6)
    durable = round(b["durable_ms"] * 1e6)
    clip = lambda iv: [(max(last_sql, a), b) for a, b in iv if b > last_sql]
    pubs = {}
    observed_req = set()
    for e in events:
        sync = e.get("state_sync", {})
        for r in sync.get("requests", []):
            if r.get("status") != "success" or r.get("clock_domain_id") not in domains:
                raise ValueError("Invalid request in state sync evidence")
            observed_req.add(r["parent_request_id"])
        for p in sync.get("publications", []):
            if p["status"] != "success" or p["clock_domain_id"] not in domains:
                raise ValueError("Invalid publication")
            pid = p["publication_id"]
            if pid in pubs and pubs[pid] != p:
                raise ValueError("Inconsistent duplicate publication")
            pubs[pid] = p
    if observed_req != set(request) or not pubs:
        raise ValueError("Missing sync evidence")
    pub_phases = collections.defaultdict(list)
    for p in pubs.values():
        for ph in p["phases"]:
            pub_phases[ph["operation"]].append((ph["start_monotonic_ns"], ph["start_monotonic_ns"] + ph["duration_ns"]))
    sweep = coverage(stmts)
    sql_overlap2 = sum(t2 - t1 for (t1, n), (t2, _) in zip(sweep, sweep[1:]) if n >= 2)
    span_by_request = {e["request_id"]: e for e in events if e.get("schema_id") == "openivm.execution-span"}
    if set(span_by_request) != set(request):
        raise ValueError("Missing completed request span")
    return {
        "label": report["label"], "batch": batch, "head": report["head_sha"], "spark": report["spark_version"],
        "role": report["role"], "workload": workload, "all_phase_clock_domains": sorted(domains),
        "workers": b["workers"], "report_sha256": hashlib.sha256(data).hexdigest(),
        "events_sha256": hashlib.sha256(raw).hexdigest(), "clock": next(iter(domains)),
        "durable_ms": b["durable_ms"], "return_ms": b["return_ms"],
        "return_to_durable_ms": b["return_to_durable_ms"], "sql_union_ms": ms(union(stmts)),
        "sql_concurrency2_ms": ms(sql_overlap2), "post_sql_to_last_phase_ms": ms(last_phase - last_sql),
        "post_sql_to_return_bounds_ms": pair_bounds(low + last_return - last_sql, high + last_return - last_sql),
        "post_sql_to_durable_bounds_ms": pair_bounds(low + durable - last_sql, high + durable - last_sql),
        "post_sql_to_durable_ms": ms(exact_origin + durable - last_sql) if exact_origin is not None else None,
        "throughput_durable_views_per_second": round(6000 / b["durable_ms"], 4),
        "throughput_return_views_per_second": round(6000 / b["return_ms"], 4),
        "median_request_durable_ms": statistics.median(r["request_durable_ms"] for r in request.values()),
        "median_submit_at_ms": statistics.median(r["submit_at_ms"] for r in request.values()),
        "request_timeline_ms": [{
            "view": r["view"], "request_id": rid, "worker_entry": r["submit_at_ms"], "bridge_start": r["bridge_start_at_ms"],
            "return": r["return_at_ms"], "durable": r["durable_at_ms"], "latency": r["request_durable_ms"],
            "same_mv_wait": span_by_request[rid].get("same_mv_lock_wait_ms"),
            "driver_admission_wait": span_by_request[rid].get("driver_admission_wait_ms"),
            "rocksdb_jvm_wait": span_by_request[rid].get("rocksdb_jvm_lock_wait_ms")
        } for rid, r in request.items()],
        "executor_slot_saturation": None,
        "phase_unions_after_last_sql_ms": {k: ms(union(v)) for k, whole in by_name.items() if (v := clip(whole))},
        "unique_publications": len(pubs), "publication_request_counts": sorted(p["requested_requests"] for p in pubs.values()),
        "publication_archive_bytes": sum(p["counts"]["archive_bytes"] for p in pubs.values()),
        "publication_phase_unions_after_last_sql_ms": {k: ms(union(v)) for k, whole in pub_phases.items()
                                                       if (v := clip(whole))},
        "gates": report["gates"], "watermarks": report["mode_proof"]["cdf_watermarks"]
    }


def coverage(intervals):
    delta = collections.Counter()
    for a, b in intervals:
        delta[a] += 1
        delta[b] -= 1
    out, active = [], 0
    for t, d in sorted(delta.items()):
        active += d
        out.append((t, active))
    return out


def task_analysis(trace, report):
    b = report["batches"]["parallel_refresh"]
    requests = set(b["request_ids"])
    origin, finish = trace["batch_start_ns"], trace["batch_end_ns"]
    if b["batch_start_monotonic_ns"] != origin or abs(ms(finish - origin) - b["durable_ms"]) > 0.002:
        raise ValueError("Task trace belongs to another batch")
    def offset(c):
        return c[1] - ((c[0] + c[2]) / 2e6)
    a, z = (offset(trace[k]) for k in ("anchor_before", "anchor_after"))
    if abs(a - z) > 3 or max(trace[k][2] - trace[k][0] for k in ("anchor_before", "anchor_after")) > 2e6:
        raise ValueError("Driver wall/monotonic cross-clock anchor moved; task window unavailable")
    start_ms, end_ms = origin / 1e6 + a, finish / 1e6 + z
    stage_owners = collections.defaultdict(set)
    jobs = [j for j in trace["jobs"] if start_ms <= j["start_epoch_ms"] <= end_ms]
    for j in jobs:
        for stage in j["stage_ids"]:
            stage_owners[stage].add(j.get("request_id"))
    tasks = [t for t in trace["tasks"] if t["finish_epoch_ms"] > start_ms and t["start_epoch_ms"] < end_ms]
    if not tasks or len({t["task"] for t in tasks}) != len(tasks):
        raise ValueError("No tasks or duplicate task attempts")
    iv = [(max(start_ms, t["start_epoch_ms"]), min(end_ms, t["finish_epoch_ms"])) for t in tasks]
    if any(b < a for a, b in iv):
        raise ValueError("Invalid task interval")
    active = union(iv)
    busy = sum(b-a for a,b in iv)
    wall = (finish - origin) / 1e6
    peak = max(n for _, n in coverage(iv))
    contained = all(t["start_epoch_ms"] >= start_ms and t["finish_epoch_ms"] <= end_ms for t in tasks)
    runtime = sum(t["run_ms"] for t in tasks) if contained and all(t["run_ms"] is not None for t in tasks) else None
    owner_count = collections.Counter()
    for t in tasks:
        owners = stage_owners[t["stage"]]
        owner = "shared" if len(owners) > 1 else "correlated" if owners & requests else "unattributed"
        owner_count[owner] += 1
    return {"slots": trace["slots"], "tasks": len(tasks), "jobs": len(jobs),
            "task_window": "all driver-observed tasks intersecting dispatch-to-durable; includes observer work",
            "clock_anchor_drift_ms": round(z-a, 4), "failed_tasks": sum(t["failed"] for t in tasks),
            "killed_tasks": sum(t["killed"] for t in tasks), "driver_task_envelope_sum_ms": round(busy, 3),
            "task_runtime_ms": runtime, "slot_runtime_fraction": round(runtime/(trace["slots"]*wall), 4) if runtime is not None else None,
            "driver_task_envelope_fraction": round(busy / (trace["slots"] * wall), 4), "peak_driver_task_envelopes": peak,
            "active_wall_ms": round(active, 3), "idle_wall_fraction": round(1 - active/wall, 4),
            "task_attribution": dict(owner_count), "executor_cpu_ms": (round(sum(t["cpu_ns"] for t in tasks) / 1e6, 3)
                                                                       if all(t["cpu_ns"] is not None for t in tasks) else None)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=pathlib.Path)
    parser.add_argument("--member-dir")
    parser.add_argument("--supplement", action="store_true")
    parser.add_argument("--output", type=pathlib.Path)
    args = parser.parse_args()
    files = (("r7-worker-order.json", "r7-worker-order-events.jsonl") if args.supplement else
             ("r7-walkthrough-cdf.json", "r7-walkthrough-events.jsonl"))
    if args.member_dir:
        with tarfile.open(args.source) as arch:
            data, events = (arch.extractfile(args.member_dir + "/" + f).read() for f in files)
    else:
        data, events = ((args.source / f).read_bytes() for f in files)
    analyses = [analyze(data, events, arm) for arm in ("serial_refresh", "parallel_refresh")]
    if args.supplement:
        if args.member_dir:
            with tarfile.open(args.source) as arch:
                ts = json.load(arch.extractfile(args.member_dir + "/r7-worker-order-tasks.json"))
        else:
            ts = json.loads((args.source / "r7-worker-order-tasks.json").read_bytes())
        analyses[1]["task_trace"] = task_analysis(ts, json.loads(data))
        analyses[1]["executor_slot_saturation"] = analyses[1]["task_trace"]["slot_runtime_fraction"]
    text = json.dumps(analyses, indent=2, allow_nan=False)
    if args.output:
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)
