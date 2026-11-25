"""Scoring of paired baseline/candidate runs. Pure functions over validated reports."""

import dataclasses
import math
import statistics
from typing import Any

import report

METRIC_LIMIT = 1.0e12


def sane(value: float) -> float:
    """Finite and within +-METRIC_LIMIT; anything else becomes 0.0."""
    value = float(value)
    if not math.isfinite(value):
        return 0.0
    return max(-METRIC_LIMIT, min(METRIC_LIMIT, value))


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def median(values: list[float]) -> float:
    return float(statistics.median(values)) if values else 0.0


def _walk(report_doc: dict[str, Any], path: str) -> float:
    node: Any = report_doc
    for part in path.split("."):
        node = node[part]
    if isinstance(node, dict):
        return float(sum(node.values()))
    return float(node)


def view_targeted(view: dict[str, Any], spec: dict[str, Any]) -> float:
    if spec["kind"] == "view_fields":
        return float(sum(view[f] for f in spec["fields"]))
    raise ValueError(f"not a per-view spec: {spec}")


def targeted_value(perf: dict[str, Any], spec: dict[str, Any]) -> float:
    if spec["kind"] == "view_fields":
        return sum(view_targeted(v, spec) for v in perf["views"])
    return _walk(perf, spec["path"])


def reduction(baseline: float, candidate: float) -> float:
    if baseline <= 0:
        return 0.0
    return (baseline - candidate) / baseline


def combined_score(e2e: float, wait: float, accepted: bool, manifest: dict[str, Any]) -> float:
    s = manifest["scoring"]
    c = float(s["ratio_clamp"])
    raw = s["e2e_weight"] * clamp(e2e, -c, c) + s["wait_weight"] * clamp(wait, -c, c)
    if accepted:
        raw += s["accepted_bonus"]
    return max(float(s["valid_floor"]), raw)


@dataclasses.dataclass
class Scores:
    errors: list[str]
    wait_reduction: float = 0.0
    e2e_improvement_median: float = 0.0
    e2e_improvement_min: float = 0.0
    e2e_improvement_max: float = 0.0
    baseline_targeted: float = 0.0
    candidate_targeted: float = 0.0
    baseline_e2e: float = 0.0
    candidate_e2e: float = 0.0
    accepted: bool = False
    concurrency_failures: list[str] = dataclasses.field(default_factory=list)
    metrics: dict[str, float] = dataclasses.field(default_factory=dict)
    view_rows: list[tuple[int, str, float, float]] = dataclasses.field(default_factory=list)
    wait_sums: dict[str, tuple[float, float]] = dataclasses.field(default_factory=dict)


def _median_of(reports: list[dict[str, Any]], getter: Any) -> float:
    return median([float(getter(r)) for r in reports])


def _view(reports: list[dict[str, Any]], index: int) -> list[dict[str, Any]]:
    return [next(v for v in r["views"] if v["mv_index"] == index) for r in reports]


def compute(
    lane: str,
    manifest: dict[str, Any],
    baselines: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
) -> Scores:
    """Score matched pairs (baselines[i] was measured against candidates[i]) for one lane."""
    spec = manifest["lanes"][lane]
    if not baselines or len(baselines) != len(candidates):
        return Scores(errors=["baseline and candidate pair counts differ"])
    profiles = {repr(r["latency_profile"]) for r in baselines + candidates}
    if len(profiles) != 1:
        return Scores(errors=["latency_profile differs between baseline and candidate runs"])

    acceptance = manifest["acceptance"]
    improvements = [
        (b["makespan_ms"] - c["makespan_ms"]) / b["makespan_ms"] for b, c in zip(baselines, candidates)
    ]
    scores = Scores(errors=[])
    scores.e2e_improvement_median = median(improvements)
    scores.e2e_improvement_min = min(improvements)
    scores.e2e_improvement_max = max(improvements)
    scores.baseline_e2e = _median_of(baselines, lambda r: r["makespan_ms"])
    scores.candidate_e2e = _median_of(candidates, lambda r: r["makespan_ms"])
    scores.baseline_targeted = median([targeted_value(r, spec["targeted"]) for r in baselines])
    scores.candidate_targeted = median([targeted_value(r, spec["targeted"]) for r in candidates])
    scores.wait_reduction = reduction(scores.baseline_targeted, scores.candidate_targeted)
    if scores.baseline_targeted <= 0:
        scores.errors.append("baseline targeted wait is zero, so no reduction can be measured")

    base_conc = median([report.engine_concurrency(r) for r in baselines])
    cand_conc = median([report.engine_concurrency(r) for r in candidates])
    base_serial = _median_of(baselines, lambda r: r["serial_sum_ms"])
    cand_serial = _median_of(candidates, lambda r: r["serial_sum_ms"])
    slack = acceptance["concurrency_slack"]
    if cand_conc < base_conc - slack:
        scores.concurrency_failures.append(
            f"max_concurrent_engine median {cand_conc:g} < baseline {base_conc:g} - {slack} (engines were serialized)"
        )
    if cand_serial < acceptance["min_serial_sum_ratio"] * base_serial and scores.candidate_e2e > scores.baseline_e2e:
        scores.concurrency_failures.append(
            f"serial_sum_ms median {cand_serial:.0f} < {acceptance['min_serial_sum_ratio']:g} x baseline "
            f"{base_serial:.0f} while makespan rose (work moved outside the measured interval)"
        )

    scores.accepted = (
        not scores.errors
        and not scores.concurrency_failures
        and scores.wait_reduction >= acceptance["min_wait_reduction"]
        and scores.e2e_improvement_median >= acceptance["min_e2e_improvement"]
    )

    metrics = scores.metrics
    metrics["engine_overlap_ms_baseline_median"] = _median_of(baselines, lambda r: r["engine_overlap_ms"])
    metrics["engine_overlap_ms_candidate_median"] = _median_of(candidates, lambda r: r["engine_overlap_ms"])
    metrics["max_concurrent_engine_baseline_median"] = base_conc
    metrics["max_concurrent_engine_candidate_median"] = cand_conc
    metrics["serial_sum_ms_baseline_median"] = base_serial
    metrics["serial_sum_ms_candidate_median"] = cand_serial
    metrics["drain_ms_candidate_median"] = _median_of(candidates, lambda r: r["drain_ms"])
    metrics["barrier_hold_overlap_ms_baseline_median"] = _median_of(baselines, lambda r: r["barrier_hold_overlap_ms"])
    metrics["barrier_hold_overlap_ms_candidate_median"] = _median_of(candidates, lambda r: r["barrier_hold_overlap_ms"])
    for info in spec["informational"]:
        metrics[f"info_{info['name']}_baseline"] = median([_walk(r, info["path"]) for r in baselines])
        metrics[f"info_{info['name']}_candidate"] = median([_walk(r, info["path"]) for r in candidates])
    for index in range(1, manifest["expected"]["views"] + 1):
        views = _view(candidates, index)
        for field in PER_MV_FIELDS:
            metrics[f"mv{index}_{field}"] = median([float(v[field]) for v in views])
        if spec["targeted"]["kind"] == "view_fields":
            base_value = median([view_targeted(v, spec["targeted"]) for v in _view(baselines, index)])
            cand_value = median([view_targeted(v, spec["targeted"]) for v in views])
            scores.view_rows.append((index, _view(candidates, index)[0]["name"], base_value, cand_value))
    for field in WAIT_FIELDS:
        scores.wait_sums[field] = (
            median([sum(v[field] for v in r["views"]) for r in baselines]),
            median([sum(v[field] for v in r["views"]) for r in candidates]),
        )
    return scores


PER_MV_FIELDS = (
    "submit_to_ack_ms",
    "state_barrier_hold_ms",
    "state_barrier_wait_ms",
    "capture_wait_ms",
    "publication_queue_wait_ms",
)
WAIT_FIELDS = (
    "state_barrier_wait_ms",
    "capture_wait_ms",
    "catalog_finalize_ms",
    "publication_queue_wait_ms",
    "publication_ms",
)
EXTRA_METRICS = (
    "engine_overlap_ms_baseline_median",
    "engine_overlap_ms_candidate_median",
    "max_concurrent_engine_baseline_median",
    "max_concurrent_engine_candidate_median",
    "serial_sum_ms_baseline_median",
    "serial_sum_ms_candidate_median",
    "drain_ms_candidate_median",
    "barrier_hold_overlap_ms_baseline_median",
    "barrier_hold_overlap_ms_candidate_median",
)


def metric_names(lane: str, manifest: dict[str, Any]) -> list[str]:
    """Every extra metric `compute` fills for a lane, in emission order."""
    names = list(EXTRA_METRICS)
    for info in manifest["lanes"][lane]["informational"]:
        names += [f"info_{info['name']}_baseline", f"info_{info['name']}_candidate"]
    for index in range(1, manifest["expected"]["views"] + 1):
        names += [f"mv{index}_{field}" for field in PER_MV_FIELDS]
    return names
