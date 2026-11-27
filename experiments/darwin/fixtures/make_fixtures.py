"""Deterministic generator for synthetic reports used by the offline tests.

Usage: python make_fixtures.py [--output-dir PATH] [--check]

Reports default to experiments/darwin/.scratch/fixtures/reports and are never tracked in Git.

The numbers are synthetic. The baseline reuses the four reference spans measured on the modeled
latency profile (MV6 capture wait 5.509 s, MV5 catalog finalization 12.439 s, MV1 publication queue
wait 5.817 s, MV6 publication 6.119 s) and an engine overlap of 31.809 s; it is not a measurement.
"""

import argparse
import copy
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
DEFAULT_OUTPUT = HERE.parent / ".scratch" / "fixtures"
EVALUATOR = HERE.parent / "evaluator"
sys.path.insert(0, str(EVALUATOR))

import report  # noqa: E402

MANIFEST = json.loads((HERE.parent / "manifest.json").read_text(encoding="utf-8"))
BASELINE_SHA = "0123456789abcdef0123456789abcdef01234567"
CANDIDATE_SHA = "89abcdef0123456789abcdef0123456789abcdef"
LATENCY = {
    "name": "modeled-v1",
    "catalog_read_ms": 40.0,
    "catalog_write_ms": 120.0,
    "state_read_ms": 35.0,
    "state_write_ms": 90.0,
    "state_rename_ms": 60.0,
    "scale": 1.0,
}

# name, barrier_wait, hold (None: spans catalog finalization), capture_wait, engine_exec,
# catalog_finalize, publication_queue_wait, publication
BASELINE_VIEWS = [
    ("sg_mv1", 200, 4000, 300, 9000, 1200, 5817, 1500),
    ("sg_mv2", 500, 4500, 400, 8500, 1500, 1200, 1800),
    ("sg_mv3", 900, 5000, 600, 9500, 2000, 900, 2100),
    ("sg_mv4", 1200, 5200, 800, 10000, 3500, 700, 2400),
    ("sg_mv5", 6000, None, 900, 25809, 12439, 600, 2600),
    ("sg_mv6", 2000, 5000, 5509, 26000, 2800, 500, 6119),
]
BASELINE_DRAIN_MS = 350.0
BASELINE_BYTES = 412_000_000
BASELINE_STATE_WRITES = 60
FIELDS = ("barrier_wait", "hold", "capture_wait", "engine_exec", "catalog_finalize", "queue_wait", "publication")


def scaled(factors: dict[str, float]) -> list[tuple]:
    rows = []
    for name, *values in BASELINE_VIEWS:
        row = [name]
        for field, value in zip(FIELDS, values):
            row.append(None if value is None else value * factors.get(field, 1.0))
        rows.append(tuple(row))
    return rows


def overlap_ms(intervals: list[tuple[float, float]]) -> float:
    events = sorted([(s, 1) for s, e in intervals if e > s] + [(e, -1) for s, e in intervals if e > s])
    total, active, last = 0.0, 0, 0.0
    for at, delta in events:
        if active >= 2:
            total += at - last
        active += delta
        last = at
    return total


def build_views(rows: list[tuple], serialize: bool = False, defer_ack: bool = False) -> list[dict]:
    views, previous_end = [], 0.0
    for index, (name, wait, hold, capture, engine, finalize, queue, publication) in enumerate(rows, start=1):
        acquired = wait
        engine_start = acquired + capture
        if serialize:
            engine_start = max(engine_start, previous_end)
        engine_end = engine_start + engine
        previous_end = engine_end
        finalize_end = engine_end + finalize
        publication_start = finalize_end + queue
        publication_end = publication_start + publication
        released = finalize_end if hold is None else acquired + hold
        ack = engine_start if defer_ack else publication_end
        views.append(
            {
                "mv_index": index,
                "name": name,
                "wall_ms": ack,
                "refresh_type": "INCREMENTAL",
                "outcome": "ok",
                "submit_to_ack_ms": ack,
                "state_barrier_wait_ms": wait,
                "state_barrier_hold_ms": released - acquired,
                "capture_wait_ms": capture,
                "catalog_finalize_ms": finalize,
                "publication_queue_wait_ms": queue,
                "publication_ms": publication,
                "engine_exec_ms": engine,
                "submit_at_ms": 0.0,
                "ack_at_ms": ack,
                "barrier_acquired_at_ms": acquired,
                "barrier_released_at_ms": released,
                "engine_start_at_ms": engine_start,
                "engine_end_at_ms": engine_end,
                "catalog_finalize_start_at_ms": engine_end,
                "catalog_finalize_end_at_ms": finalize_end,
                "publication_start_at_ms": publication_start,
                "publication_end_at_ms": publication_end,
            }
        )
    return views


def perf_doc(views: list[dict], drain: float, bytes_uploaded: int, state_writes: int, sha: str) -> dict:
    engines = [(v["engine_start_at_ms"], v["engine_end_at_ms"]) for v in views]
    holds = [(v["barrier_acquired_at_ms"], v["barrier_released_at_ms"]) for v in views]
    last_end = max(max(v["ack_at_ms"], v["publication_end_at_ms"]) for v in views)
    makespan = last_end + drain
    return {
        "schema": report.PERF_SCHEMA,
        "evidence_class": report.EVIDENCE_CLASS,
        "spark_target": "spark-3.5",
        "head_sha": sha,
        "latency_profile": dict(LATENCY),
        "end_to_end_ms": makespan,
        "makespan_ms": makespan,
        "drain_ms": drain,
        "serial_sum_ms": sum(v["submit_to_ack_ms"] for v in views),
        "engine_overlap_ms": overlap_ms(engines),
        "max_concurrent_engine": report.max_concurrent(engines),
        "barrier_hold_overlap_ms": overlap_ms(holds),
        "views": views,
        "counts": {
            "incremental_count": 6,
            "full_refresh_count": 0,
            "fs_ops": {
                "state_sync": {"read": 24, "write": state_writes, "rename": 18, "delete": 12, "list": 30},
                "catalog": {"read": 180, "write": 96, "rename": 12, "delete": 6, "list": 48},
            },
            "state_sync": {"publications": 18, "commits": 18, "snapshots": 6, "bytes_uploaded": bytes_uploaded},
        },
        "gates": {"parity": True, "incremental_only": True, "state_sync_consistent": True},
        "notes": ["synthetic fixture generated by make_fixtures.py; not a measurement"],
    }


def baseline_perf() -> dict:
    return perf_doc(build_views(scaled({})), BASELINE_DRAIN_MS, BASELINE_BYTES, BASELINE_STATE_WRITES, BASELINE_SHA)


GOOD_FACTORS = {
    "barrier_wait": 0.1,
    "hold": 0.1,
    "capture_wait": 0.05,
    "engine_exec": 0.15,
    "catalog_finalize": 0.1,
    "queue_wait": 0.05,
    "publication": 0.1,
}


def good_perf() -> dict:
    return perf_doc(build_views(scaled(GOOD_FACTORS)), 120.0, BASELINE_BYTES // 10, 18, CANDIDATE_SHA)


CRASH_POINTS = (
    "state_sync_before_snapshot_commit",
    "state_sync_after_snapshot_commit",
    "refresh_before_finalize",
    "acked_refresh_durable",
)
KNOWN_STALE_SNAPSHOT = "intercept/state_sync_before_snapshot_commit"


def crash_doc(failing: dict[str, tuple[str, ...]] | None = None, dropped: tuple[str, ...] = ()) -> dict:
    """The eight gated crash points plus the documented known gap; `failing` maps "mode/name" to its false flags."""
    failing = failing or {}
    docs = []
    for mode in MANIFEST["expected"]["crash_modes"]:
        for name in CRASH_POINTS:
            key = f"{mode}/{name}"
            if key in dropped:
                continue
            flags = {flag: flag not in failing.get(key, ()) for flag in report.CRASH_FLAGS}
            docs.append({"name": name, "mode": mode, **flags, "known_gap": False, "notes": f"{mode} mode: {name}"})
    docs.append(
        {
            "name": MANIFEST["known_gap"]["point"],
            "mode": "cdf",
            **{flag: False for flag in report.CRASH_FLAGS},
            "known_gap": True,
            "notes": "documented known gap; not exercised",
        }
    )
    return {"schema": report.CRASH_SCHEMA, "points": docs}


STALE_SNAPSHOT_FLAGS = {KNOWN_STALE_SNAPSHOT: ("parity", "metadata_converged", "incremental_after_recovery")}
WITNESS_OUTCOMES = {f"{case['spec']}: {case['test']}": "passed" for case in MANIFEST["witness_cases"]}


def witness_outcomes(**overrides: str | None) -> dict[str, str]:
    """All witness cases passed, except `overrides` keyed by case name: a status, or None for "did not run"."""
    by_name = {case["name"]: f"{case['spec']}: {case['test']}" for case in MANIFEST["witness_cases"]}
    outcomes = dict(WITNESS_OUTCOMES)
    for name, status in overrides.items():
        if status is None:
            del outcomes[by_name[name]]
        else:
            outcomes[by_name[name]] = status
    return outcomes


def lane_a_changes() -> list[dict]:
    lane = MANIFEST["lanes"]["a"]
    return [
        {"path": lane["owned"][0], "status": "M", "old_mode": "100644", "new_mode": "100644", "binary": False},
        {"path": lane["shared"][0], "status": "M", "old_mode": "100644", "new_mode": "100644", "binary": False},
    ]


def report_sets() -> dict[str, dict[str, object]]:
    base, good = baseline_perf(), good_perf()
    sets: dict[str, dict[str, object]] = {
        "baseline": {"perf": base, "crash": crash_doc(STALE_SNAPSHOT_FLAGS), "run": {"exit_codes": {"perf": 0, "crash": 1}}},
        "good-candidate": {"perf": good, "crash": crash_doc()},
    }

    only_a = scaled({"barrier_wait": 0.1, "capture_wait": 0.05})
    sets["lane-a-only"] = {
        "perf": perf_doc(build_views(only_a), BASELINE_DRAIN_MS, BASELINE_BYTES, BASELINE_STATE_WRITES, CANDIDATE_SHA),
        "crash": crash_doc(),
        "changes": lane_a_changes(),
    }

    slow = scaled({f: 1.2 for f in FIELDS})
    sets["slow-valid"] = {
        "perf": perf_doc(build_views(slow), BASELINE_DRAIN_MS * 1.2, BASELINE_BYTES, BASELINE_STATE_WRITES, CANDIDATE_SHA),
        "crash": crash_doc(),
    }

    bad_parity = copy.deepcopy(good)
    bad_parity["gates"]["parity"] = False
    sets["bad-parity"] = {
        "perf": bad_parity,
        "crash": crash_doc(),
        "run": {
            "exit_codes": {"perf": 1, "crash": 0},
            "log": [
                "[info] Run completed in 8 minutes.",
                "[info] - MV6 matches the EXCEPT ALL oracle *** FAILED ***",
                "[info]   1 did not equal 0 (StateGuardPerfSpec.scala:212)",
                "[error] Failed: Total 5, Failed 1, Errors 0, Passed 4",
            ],
        },
    }

    sets["bad-crash"] = {
        "perf": good,
        "crash": crash_doc({"cdf/refresh_before_finalize": ("recovered", "metadata_converged")}),
        "run": {"exit_codes": {"perf": 0, "crash": 1}},
    }
    sets["unfixed-recovery"] = {
        "perf": good,
        "crash": crash_doc(STALE_SNAPSHOT_FLAGS),
        "run": {"exit_codes": {"perf": 0, "crash": 1}},
    }
    sets["crash-point-dropped"] = {
        "perf": good,
        "crash": crash_doc(dropped=("cdf/acked_refresh_durable",)),
    }
    sets["witness-failed"] = {
        "perf": good,
        "crash": crash_doc(),
        "run": {"exit_codes": {"perf": 0, "crash": 1}},
        "outcomes": witness_outcomes(fresh_jvm_delta_post_input_snapshot="failed"),
    }
    sets["witness-not-run"] = {
        "perf": good,
        "crash": crash_doc(),
        "run": {"exit_codes": {"perf": 0, "crash": 1}},
        "outcomes": witness_outcomes(legacy_intercept_bootstrap_stays_incremental=None, legacy_intercept_unreadable_identity_fails="skipped"),
    }

    regression = copy.deepcopy(good)
    regression["views"][3]["refresh_type"] = "FULL_REFRESH"
    regression["counts"]["incremental_count"] = 5
    regression["counts"]["full_refresh_count"] = 1
    regression["gates"]["incremental_only"] = False
    sets["full-refresh-regression"] = {"perf": regression, "crash": crash_doc()}

    nonfinite = copy.deepcopy(good)
    nonfinite["views"][2]["wall_ms"] = float("nan")
    sets["nonfinite"] = {"perf": nonfinite, "crash": crash_doc()}

    wrong_class = copy.deepcopy(good)
    wrong_class["evidence_class"] = "onelake-measured"
    sets["wrong-evidence-class"] = {"perf": wrong_class, "crash": crash_doc()}

    serialized = scaled({"barrier_wait": 0.05, "capture_wait": 0.05, "catalog_finalize": 0.05, "queue_wait": 0.05, "publication": 0.05, "engine_exec": 0.2})
    sets["serialized-engines"] = {
        "perf": perf_doc(build_views(serialized, serialize=True), 100.0, BASELINE_BYTES // 10, 18, CANDIDATE_SHA),
        "crash": crash_doc(),
    }

    sets["deferred-work"] = {
        "perf": perf_doc(build_views(scaled({"barrier_wait": 0.1, "capture_wait": 0.05}), defer_ack=True), BASELINE_DRAIN_MS * 40, BASELINE_BYTES, BASELINE_STATE_WRITES, CANDIDATE_SHA),
        "crash": crash_doc(),
    }

    owned = MANIFEST["lanes"]["a"]["owned"][0]
    sets["bad-allowlist"] = {
        "perf": good,
        "crash": crash_doc(),
        "changes": [
            {"path": owned, "status": "M", "old_mode": "100644", "new_mode": "100644", "binary": False},
            {
                "path": "spark-ext/ivm-it/src/test/scala/org/openivm/spark/perf/StateGuardPerfSpec.scala",
                "status": "M",
                "old_mode": "100644",
                "new_mode": "100644",
                "binary": False,
            },
        ],
    }
    return sets


def dump(document: object) -> str:
    return json.dumps(document, indent=1) + "\n"


def generate() -> dict[str, str]:
    files: dict[str, str] = {}
    for name, parts in report_sets().items():
        folder = f"reports/{name}"
        files[f"{folder}/state-guard-perf.json"] = dump(parts["perf"])
        files[f"{folder}/state-guard-crash.json"] = dump(parts["crash"])
        files[f"{folder}/run.json"] = dump({**parts.get("run", {}), "outcomes": parts.get("outcomes", WITNESS_OUTCOMES)})
        if "changes" in parts:
            files[f"{folder}/changes.json"] = dump(parts["changes"])
    return files


def write(root: Path = DEFAULT_OUTPUT) -> None:
    for rel, text in generate().items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8", newline="\n")


def check(root: Path = DEFAULT_OUTPUT) -> list[str]:
    expected = generate()
    problems = []
    for rel, text in expected.items():
        path = root / rel
        if not path.is_file():
            problems.append(f"{rel}: missing")
        elif path.read_bytes().replace(b"\r\n", b"\n").decode("utf-8") != text:
            problems.append(f"{rel}: stale")
    on_disk = {p.relative_to(root).as_posix() for p in (root / "reports").rglob("*.json")}
    problems += [f"{rel}: unexpected file" for rel in sorted(on_disk - set(expected))]
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT, help="scratch root containing reports/")
    parser.add_argument("--check", action="store_true", help="verify generated reports in the output directory")
    args = parser.parse_args(argv)
    if args.check:
        problems = check(args.output_dir)
        for problem in problems:
            print(problem, file=sys.stderr)
        return 1 if problems else 0
    write(args.output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
