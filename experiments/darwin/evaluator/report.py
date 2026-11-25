"""Validation of the trusted state-guard suite reports (perf and crash, schema v1)."""

import json
import math
import re
from typing import Any

PERF_SCHEMA = "openivm.state-guard-perf/v1"
CRASH_SCHEMA = "openivm.state-guard-crash/v1"
EVIDENCE_CLASS = "local-relative-modeled-latency"

LATENCY_FIELDS = (
    "catalog_read_ms",
    "catalog_write_ms",
    "state_read_ms",
    "state_write_ms",
    "state_rename_ms",
    "scale",
)
VIEW_DURATIONS = (
    "wall_ms",
    "submit_to_ack_ms",
    "state_barrier_wait_ms",
    "state_barrier_hold_ms",
    "capture_wait_ms",
    "catalog_finalize_ms",
    "publication_queue_wait_ms",
    "publication_ms",
    "engine_exec_ms",
)
VIEW_OFFSETS = (
    "submit_at_ms",
    "ack_at_ms",
    "barrier_acquired_at_ms",
    "barrier_released_at_ms",
    "engine_start_at_ms",
    "engine_end_at_ms",
    "catalog_finalize_start_at_ms",
    "catalog_finalize_end_at_ms",
    "publication_start_at_ms",
    "publication_end_at_ms",
)
OFFSET_PAIRS = (
    ("submit_at_ms", "ack_at_ms"),
    ("barrier_acquired_at_ms", "barrier_released_at_ms"),
    ("engine_start_at_ms", "engine_end_at_ms"),
    ("catalog_finalize_start_at_ms", "catalog_finalize_end_at_ms"),
    ("publication_start_at_ms", "publication_end_at_ms"),
)
TOP_NUMBERS = (
    "end_to_end_ms",
    "makespan_ms",
    "drain_ms",
    "serial_sum_ms",
    "engine_overlap_ms",
    "barrier_hold_overlap_ms",
)
FS_OPS = ("read", "write", "rename", "delete", "list")
FS_GROUPS = ("state_sync", "catalog")
STATE_SYNC_COUNTS = ("publications", "commits", "snapshots", "bytes_uploaded")
GATE_FLAGS = ("parity", "incremental_only", "state_sync_consistent")
CRASH_FLAGS = ("crashed", "recovered", "parity", "metadata_converged", "incremental_after_recovery")
FAILED_OUTCOME = re.compile(r"fail|error|exception|abort", re.IGNORECASE)


def _reject_constant(name: str) -> Any:
    raise ValueError(f"non-finite JSON constant {name}")


def load_json(path: Any) -> Any:
    with open(path, "r", encoding="utf-8") as handle:
        return json.loads(handle.read(), parse_constant=_reject_constant)


def is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def normalize_refresh_type(value: str) -> str:
    return re.sub(r"[\s\-]+", "_", value.strip()).upper()


def is_full_refresh(refresh_type: str) -> bool:
    return normalize_refresh_type(refresh_type) == "FULL_REFRESH"


def max_concurrent(intervals: list[tuple[float, float]]) -> int:
    """Peak number of simultaneously open intervals; touching intervals do not overlap."""
    events: list[tuple[float, int]] = []
    for start, end in intervals:
        if end > start:
            events.append((start, 1))
            events.append((end, -1))
    events.sort(key=lambda event: (event[0], event[1]))
    peak = current = 0
    for _, delta in events:
        current += delta
        peak = max(peak, current)
    return peak


def engine_concurrency(perf: dict[str, Any]) -> int:
    return max_concurrent(
        [(float(v["engine_start_at_ms"]), float(v["engine_end_at_ms"])) for v in perf["views"]]
    )


class _Checker:
    def __init__(self, limit: float) -> None:
        self.errors: list[str] = []
        self.limit = limit

    def number(self, container: Any, key: str, where: str, positive: bool = False) -> float | None:
        if not isinstance(container, dict) or key not in container:
            self.errors.append(f"{where}.{key}: missing")
            return None
        value = container[key]
        if not is_number(value):
            self.errors.append(f"{where}.{key}: not a number ({type(value).__name__})")
            return None
        if not math.isfinite(value):
            self.errors.append(f"{where}.{key}: not finite")
            return None
        if value < 0 or (positive and value == 0):
            self.errors.append(f"{where}.{key}: must be {'> 0' if positive else '>= 0'} (got {value})")
            return None
        if value > self.limit:
            self.errors.append(f"{where}.{key}: implausibly large ({value})")
            return None
        return float(value)

    def integer(self, container: Any, key: str, where: str) -> int | None:
        value = self.number(container, key, where)
        if value is None:
            return None
        if not is_int(container[key]):
            self.errors.append(f"{where}.{key}: must be an integer")
            return None
        return int(value)

    def string(self, container: Any, key: str, where: str) -> str | None:
        value = container.get(key) if isinstance(container, dict) else None
        if not isinstance(value, str) or not value.strip():
            self.errors.append(f"{where}.{key}: must be a non-empty string")
            return None
        return value


def validate_perf(perf: Any, spark_target: str, manifest: dict[str, Any]) -> list[str]:
    limit = float(manifest["expected"]["max_report_value"])
    check = _Checker(limit)
    if not isinstance(perf, dict):
        return ["perf report: top level must be an object"]
    if perf.get("schema") != PERF_SCHEMA:
        check.errors.append(f"schema: expected {PERF_SCHEMA!r}, got {perf.get('schema')!r}")
    if perf.get("evidence_class") != EVIDENCE_CLASS:
        check.errors.append(
            f"evidence_class: expected {EVIDENCE_CLASS!r}, got {perf.get('evidence_class')!r}"
        )
    if perf.get("spark_target") != spark_target:
        check.errors.append(f"spark_target: expected {spark_target!r}, got {perf.get('spark_target')!r}")
    if not isinstance(perf.get("head_sha"), str):
        check.errors.append("head_sha: must be a string")

    profile = perf.get("latency_profile")
    if not isinstance(profile, dict):
        check.errors.append("latency_profile: missing")
    else:
        check.string(profile, "name", "latency_profile")
        for field in LATENCY_FIELDS:
            check.number(profile, field, "latency_profile")

    top = {name: check.number(perf, name, "report", positive=name in ("end_to_end_ms", "makespan_ms")) for name in TOP_NUMBERS}
    concurrent = check.integer(perf, "max_concurrent_engine", "report")

    makespan, e2e = top["makespan_ms"], top["end_to_end_ms"]
    if makespan is not None and e2e is not None and abs(makespan - e2e) > 1e-6 * max(makespan, 1.0) + 1e-3:
        check.errors.append(f"end_to_end_ms ({e2e}) must equal makespan_ms ({makespan})")

    views = perf.get("views")
    expected_views = int(manifest["expected"]["views"])
    if not isinstance(views, list) or len(views) != expected_views:
        check.errors.append(f"views: expected exactly {expected_views} entries")
        views = []
    indexes = []
    ack_sum = 0.0
    latest_ack = 0.0
    for position, view in enumerate(views):
        where = f"views[{position}]"
        if not isinstance(view, dict):
            check.errors.append(f"{where}: must be an object")
            continue
        index = check.integer(view, "mv_index", where)
        indexes.append(index)
        check.string(view, "name", where)
        check.string(view, "refresh_type", where)
        check.string(view, "outcome", where)
        values = {name: check.number(view, name, where) for name in VIEW_DURATIONS + VIEW_OFFSETS}
        for start, end in OFFSET_PAIRS:
            if values[start] is not None and values[end] is not None and values[end] < values[start]:
                check.errors.append(f"{where}: {end} precedes {start}")
        if values["submit_to_ack_ms"] is not None:
            ack_sum += values["submit_to_ack_ms"]
        if values["ack_at_ms"] is not None:
            latest_ack = max(latest_ack, values["ack_at_ms"])
        submit, ack, span = values["submit_at_ms"], values["ack_at_ms"], values["submit_to_ack_ms"]
        if None not in (submit, ack, span) and abs((ack - submit) - span) > max(100.0, 0.1 * span):
            check.errors.append(f"{where}: submit_to_ack_ms disagrees with ack_at_ms - submit_at_ms")
    if views and sorted(i for i in indexes if i is not None) != list(range(1, expected_views + 1)):
        check.errors.append(f"views: mv_index must be exactly 1..{expected_views}")
    serial = top["serial_sum_ms"]
    if serial is not None and abs(serial - ack_sum) > max(1.0, 1e-3 * ack_sum):
        check.errors.append(f"serial_sum_ms ({serial}) must equal the sum of submit_to_ack_ms ({ack_sum})")
    if makespan is not None and views and makespan + 1.0 < latest_ack:
        check.errors.append("makespan_ms ends before the last durable ack")
    if concurrent is not None and views and not check.errors:
        recomputed = engine_concurrency(perf)
        if concurrent != recomputed:
            check.errors.append(
                f"max_concurrent_engine ({concurrent}) disagrees with engine offsets ({recomputed})"
            )

    counts = perf.get("counts")
    if not isinstance(counts, dict):
        check.errors.append("counts: missing")
    else:
        check.integer(counts, "incremental_count", "counts")
        check.integer(counts, "full_refresh_count", "counts")
        fs_ops = counts.get("fs_ops")
        for group in FS_GROUPS:
            for op in FS_OPS:
                check.integer(fs_ops.get(group) if isinstance(fs_ops, dict) else None, op, f"counts.fs_ops.{group}")
        for name in STATE_SYNC_COUNTS:
            check.integer(counts.get("state_sync"), name, "counts.state_sync")

    gates = perf.get("gates")
    if not isinstance(gates, dict):
        check.errors.append("gates: missing")
    else:
        for flag in GATE_FLAGS:
            if not isinstance(gates.get(flag), bool):
                check.errors.append(f"gates.{flag}: must be a boolean")
    notes = perf.get("notes")
    if not isinstance(notes, list) or not all(isinstance(n, str) for n in notes):
        check.errors.append("notes: must be a list of strings")
    return check.errors


def perf_gate_failures(perf: dict[str, Any], manifest: dict[str, Any]) -> dict[str, list[str]]:
    """Semantic gate failures of a structurally valid perf report, keyed by gate name."""
    correctness: list[str] = []
    incremental: list[str] = []
    gates = perf["gates"]
    if not gates["parity"]:
        correctness.append("gates.parity is false (MV result differs from the oracle)")
    if not gates["state_sync_consistent"]:
        correctness.append("gates.state_sync_consistent is false")
    for view in perf["views"]:
        if FAILED_OUTCOME.search(view["outcome"]):
            correctness.append(f"MV{view['mv_index']} outcome {view['outcome']!r}")
    counts = perf["counts"]
    expected = manifest["expected"]
    if counts["full_refresh_count"] != expected["full_refresh_count"]:
        incremental.append(f"full_refresh_count={counts['full_refresh_count']} (must be 0)")
    if counts["incremental_count"] != expected["incremental_count"]:
        incremental.append(f"incremental_count={counts['incremental_count']} (must be {expected['incremental_count']})")
    if not gates["incremental_only"]:
        incremental.append("gates.incremental_only is false")
    for view in perf["views"]:
        if is_full_refresh(view["refresh_type"]):
            incremental.append(f"MV{view['mv_index']} ({view['name']}) fell back to FULL_REFRESH")
    return {"correctness": correctness, "incremental": incremental}


def validate_crash(crash: Any, manifest: dict[str, Any]) -> list[str]:
    if not isinstance(crash, dict):
        return ["crash report: top level must be an object"]
    errors: list[str] = []
    if crash.get("schema") != CRASH_SCHEMA:
        errors.append(f"schema: expected {CRASH_SCHEMA!r}, got {crash.get('schema')!r}")
    points = crash.get("points")
    if not isinstance(points, list):
        return errors + ["points: must be a list"]
    minimum = int(manifest["expected"]["min_crash_points"])
    if len(points) < minimum:
        errors.append(f"points: need at least {minimum}, got {len(points)}")
    for position, point in enumerate(points):
        where = f"points[{position}]"
        if not isinstance(point, dict):
            errors.append(f"{where}: must be an object")
            continue
        if not isinstance(point.get("name"), str) or not point["name"]:
            errors.append(f"{where}.name: must be a non-empty string")
        for flag in CRASH_FLAGS + ("known_gap",):
            if not isinstance(point.get(flag), bool):
                errors.append(f"{where}.{flag}: must be a boolean")
        if not isinstance(point.get("notes"), str):
            errors.append(f"{where}.notes: must be a string")
    return errors


def crash_gate(crash: dict[str, Any]) -> tuple[list[str], list[str]]:
    """Return (failures, documented known-gap point names) for a validated crash report."""
    failures: list[str] = []
    known: list[str] = []
    for point in crash["points"]:
        if point["known_gap"]:
            known.append(point["name"])
            continue
        missing = [flag for flag in CRASH_FLAGS if not point[flag]]
        if missing:
            failures.append(f"crash point {point['name']!r}: {', '.join(missing)} false")
    return failures, known
