"""Paired analysis of CdfObserverProbe arms: python3 cdf_arms_analyze.py <arms-dir> <out.json>."""
import glob
import json
import os
import re
import statistics
import sys

d, out = sys.argv[1], sys.argv[2]
runs = {}
for f in sorted(glob.glob(os.path.join(d, "*.json"))):
    m = re.search(r"-p(\d+)-(off|legacy|deep)\.json$", f)
    if not m:
        continue
    txt = open(f).read().strip()
    runs[(int(m.group(1)), m.group(2))] = json.loads(txt) if txt else None

order = [l.split() for l in open(os.path.join(d, "order.txt")) if l.strip() and l.strip() != "DONE"]
perms = sorted({p for p, _ in runs})
complete = [p for p in perms if all(runs.get((p, a)) for a in ("off", "legacy", "deep"))]


def batch_metrics(r):
    res = {}
    total_batch = total_return = 0
    for b in r["measured"]["batches"]:
        cmds = b["commands"]
        ph = b["phase"]
        res[ph + ".batch_ns"] = b["batch_ns"]
        res[ph + ".last_return_ns"] = b["last_return_ns"]
        res[ph + ".command_sum_ns"] = b["command_sum_ns"]
        res[ph + ".capture_sum_ns"] = sum(c["capture_ns"] - c["return_ns"] for c in cmds)
        res[ph + ".drain_release_sum_ns"] = sum(c["done_ns"] - c["capture_ns"] for c in cmds)
        res[ph + ".drain_release_max_ns"] = max(c["done_ns"] - c["capture_ns"] for c in cmds)
        res[ph + ".drain_timeouts"] = b["drain_timeouts"]
        total_batch += b["batch_ns"]
        total_return += b["last_return_ns"]
    res["all4.batch_ns"] = total_batch
    res["all4.last_return_ns"] = total_return
    return res


def stats(xs):
    return {"n": len(xs), "median": statistics.median(xs), "min": min(xs), "max": max(xs), "values": xs}


per_arm = {}
for (p, a), r in sorted(runs.items()):
    if not r:
        per_arm[f"p{p}-{a}"] = {"status": "missing_result"}
        continue
    bm = batch_metrics(r)
    effective = sorted({t for b in r["measured"]["batches"] for c in b["commands"] for t in c["effective_refresh_types"]})
    per_arm[f"p{p}-{a}"] = {
        "metrics": bm,
        "effective_refresh_types": effective,
        "census": r["census"],
        "queue": r.get("queue"),
        "inventory_counts": {k: v for k, v in r["inventory"].items() if not isinstance(v, dict)},
        "inventory_event_types": r["inventory"]["event_types"],
        "task_summary_status": r["inventory"]["task_summary_status"],
        "querylog_release_retries_all_cycles": r.get("querylog_release_retries_all_cycles"),
        "final_drain": r["final_drain"],
        "spark_version": r["spark_version"],
        "java_version": r["java_version"],
    }

keys = sorted(batch_metrics(runs[(complete[0], "deep")]).keys()) if complete else []
ratio_keys = [k for k in keys if k.endswith(("batch_ns", "last_return_ns", "command_sum_ns"))]
pairs = {}
for num, den in (("deep", "legacy"), ("deep", "off"), ("legacy", "off")):
    for k in ratio_keys:
        xs = [batch_metrics(runs[(p, num)])[k] / batch_metrics(runs[(p, den)])[k] for p in complete]
        pairs[f"{num}/{den}:{k}"] = stats(xs)
absolute = {}
for a in ("off", "legacy", "deep"):
    for k in keys:
        absolute[f"{a}:{k}"] = stats([batch_metrics(runs[(p, a)])[k] for p in complete])

json.dump(
    {
        "arms_dir": d,
        "permutations_complete": complete,
        "order": [" ".join(x) for x in order],
        "paired_ratios": pairs,
        "absolute": absolute,
        "per_arm": per_arm,
    },
    open(out, "w"),
    indent=1,
)

print(f"complete permutations: {complete}")
print("ratio (median [min,max]) over permutations")
for k in sorted(pairs):
    if k.split(":")[1].endswith(("batch_ns", "last_return_ns")):
        s = pairs[k]
        print(f"  {k:55s} {s['median']:.4f} [{s['min']:.4f}, {s['max']:.4f}]")
print("absolute seconds (median [min,max])")
for k in sorted(absolute):
    if k.split(":")[1].endswith(("batch_ns", "last_return_ns", "drain_release_max_ns")):
        s = absolute[k]
        print(f"  {k:55s} {s['median']/1e9:.3f} [{s['min']/1e9:.3f}, {s['max']/1e9:.3f}]")
for name, v in per_arm.items():
    if "metrics" not in v:
        print(name, v)
        continue
    c, q, i = v["census"], v["queue"] or {}, v["inventory_counts"]
    print(
        f"{name}: types={v['effective_refresh_types']} jobs={c['measured_jobs']} req_jobs={c['measured_jobs_with_request_property']}"
        f" tasks={c['measured_tasks']} published={q.get('measured_published')} drops="
        f"{sum(q.get(x, 0) for x in ('dropped_full','dropped_oversized','dropped_closed','dropped_contended'))}"
        f" records={i.get('records')} bytes={i.get('jsonl_bytes')} stage_rec={i.get('stage_completed')}"
        f" dup={i.get('duplicate_stage_attempt_records')} trunc={i.get('job_starts_stage_truncated')}"
        f" drain_to={sum(v['metrics'][k] for k in v['metrics'] if k.endswith('drain_timeouts'))}"
        f" retries={v['querylog_release_retries_all_cycles']} final_drain={v['final_drain']}"
    )
