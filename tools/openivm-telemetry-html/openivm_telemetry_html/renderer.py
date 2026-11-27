"""Pure HTML rendering for a Fabric guided lab; no Fabric dependencies.

Adapter contract (UI-only; not the library's schema): report is a mapping with
operation_id, operation_name, status, wrapper_elapsed_ms, library_elapsed_ms,
events, provenance, before_rows, after_rows, notices, errors, dropped_events,
and optionally parallel_task_time_ms. Missing values are unknown, not zero.
An event is a mapping with phase, span_id, parent_span_id, status, start_ms,
end_ms, inclusive_ms, exclusive_ms, exclusive_scope, metrics, sql, and plan.
start_ms/end_ms are finite non-negative milliseconds relative to the wrapper
operation's start, mapped by the caller from the source clock domain; end may
be absent while running. inclusive_ms and exclusive_ms are optional measured
library durations, not inferred offsets. exclusive_scope describes what the
library's exclusive measurement covers; it is not assumed to be wall time.
phase_id/parent_phase_id map to span_id/parent_span_id. For separate clock
domains the adapter must not supply offsets until they can be mapped to the
same wrapper clock. A library exclusive value is displayed only when finite,
non-negative, and no greater than the known inclusive duration.
library_elapsed_ms is the library's measured inclusive elapsed time, not a sum
of spans. parallel_task_time_ms is measured aggregate task time, not wall time.
provenance and metrics are mappings; rows are sequences of mappings or values;
notices and errors are sequences of text. dropped_events is a non-negative
count of events lost upstream. A producer must supply only measured telemetry;
the adapter must map the verified API to this contract rather than invent it.
"""

from collections.abc import Mapping, Sequence
from html import escape
import math
import re


MAX_SPANS = 100
MAX_ROWS = 20
MAX_FIELDS = 20
MAX_MESSAGES = 20
MAX_TEXT = 12000

_STYLE = """
<style>
.lab{font:14px/1.5 "Segoe UI",sans-serif;color:#242424;background:#fff;
 border:1px solid #8bb9df;border-radius:8px;max-width:1100px;margin:12px auto}
.lab *{box-sizing:border-box}.lab header,.lab section{padding:16px 20px}
.lab header{background:#f2f8fd;border-bottom:1px solid #d5e4ef}
.lab h1{font-size:21px;line-height:1.25;margin:3px 0 6px}
.lab h2{font-size:16px;margin:0 0 8px}.lab p{margin:5px 0}
.lab .eyebrow{font-size:11px;font-weight:700;letter-spacing:.08em;
 text-transform:uppercase;color:#315b7d}
.lab .muted{color:#5d6670}.lab .badge{display:inline-block;border:1px solid #aebdca;
 background:#fff;border-radius:12px;padding:1px 9px;font-weight:600}
.lab .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:8px}
.lab .metric{border:1px solid #d7e1e8;border-radius:6px;padding:9px 11px;background:#fff}
.lab .metric strong{display:block;font-size:16px}.lab .metric span{color:#566473;font-size:12px}
.lab .wall-duration{display:flex;align-items:baseline;justify-content:space-between;gap:12px;
 border:1px solid #bdd4e5;border-radius:7px;background:#fff;padding:9px 13px;margin:12px 0}
.lab .wall-duration span{font-size:12px;font-weight:600;color:#536575}
.lab .wall-duration strong{font-size:25px;line-height:1.15;font-variant-numeric:tabular-nums}
.lab section+section{border-top:1px solid #e3eaf0}
.lab details{border:1px solid #d7e1e8;border-radius:6px;margin:7px 0}
.lab summary{cursor:pointer;padding:9px 11px;font-weight:600}
.lab details>div{padding:0 11px 11px}.lab pre{white-space:pre-wrap;overflow-wrap:anywhere;
 background:#f6f8fa;border:1px solid #e1e7ec;padding:10px;border-radius:4px}
.lab table{border-collapse:collapse;width:100%;font-size:12px}
.lab th,.lab td{border-bottom:1px solid #e4ebf0;padding:6px;text-align:left;
 vertical-align:top;overflow-wrap:anywhere}.lab th{color:#536575}
.lab .scroll{overflow-x:auto}.lab .note{border-left:3px solid #6aa9d7;padding-left:9px}
.lab .track{position:relative;height:7px;background:#edf3f7;border-radius:5px;margin:3px 0}
.lab .bar{position:absolute;height:7px;background:#518dbe;border-radius:5px}
.lab .phase-table{table-layout:fixed}.lab .phase-table th:first-child{width:37%}
.lab .phase-table th:last-child{width:105px;text-align:right}
.lab .phase-table td{padding:5px 7px;vertical-align:middle}
.lab .phase-name{font-weight:600}.lab .phase-description{display:block;color:#66727d;font-size:11px;line-height:1.3}
.lab .phase-duration{text-align:right;white-space:nowrap;font-variant-numeric:tabular-nums;font-weight:600}
.lab .outcome{border:1px solid;border-radius:6px;padding:8px 11px;margin:10px 0}
.lab .outcome strong{display:block}.lab .outcome code{font-size:12px}
.lab .outcome--incremental{border-color:#86bba0;background:#f0f8f3;color:#205d3e}
.lab .outcome--full{border-color:#d6a25f;background:#fff7eb;color:#714600}
.lab .outcome--noop{border-color:#bdc8d1;background:#f4f6f8;color:#46535e}
.lab .outcome--other{border-color:#c8d7e2;background:#f7fafc;color:#344b5c}
.lab .outcome-reason{font-size:12px;margin:4px 0 0}
.lab .cdf-summary{border:1px solid #bdd4e5;border-radius:7px;background:#f7fbfe;padding:9px 12px;margin:9px 0}
.lab .cdf-summary>strong{display:block;font-size:12px;color:#315b7d;margin-bottom:5px}
.lab .cdf-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(125px,1fr));gap:7px}
.lab .cdf-item{font-size:11px;color:#566473}.lab .cdf-item strong{display:block;color:#242424;
 font-size:13px;font-variant-numeric:tabular-nums;overflow-wrap:anywhere}
.lab .consumption-evidence{border:1px solid #d7e1e8;border-radius:7px;background:#fff;padding:8px 11px;margin:9px 0}
.lab .consumption-evidence>strong{font-size:12px;color:#315b7d}
.lab .source-version{font-size:12px;padding:4px 0;border-top:1px solid #e4ebf0}
.lab .source-version:first-of-type{border-top:0}
.lab .batch-count{display:inline-block;margin-top:5px;border-radius:12px;background:#edf3f7;
 padding:2px 8px;font-size:12px;font-variant-numeric:tabular-nums}
.lab .sql-record{border-top:1px solid #e4ebf0;padding-top:8px;margin-top:8px}
.lab .sql-record:first-child{border-top:0;margin-top:0}
.lab .sql-meta{font-size:12px;color:#586774}
.lab .sql-code{font:12px/1.55 Consolas,"Courier New",monospace;tab-size:2}
.lab .sql-keyword{color:#005a9e;font-weight:600}
.lab .sql-string{color:#a4262c}.lab .sql-comment{color:#56705a}
.lab .sql-number{color:#795e26}.lab .sql-identifier{color:#624084}
.lab ul{margin:4px 0;padding-left:20px}.lab code{overflow-wrap:anywhere}
@media(max-width:600px){.lab .phase-table th:first-child{width:44%}
 .lab .phase-table th:last-child{width:82px}.lab .wall-duration strong{font-size:21px}}
</style>
"""


def _text(value):
    if value is None:
        return "Unknown"
    text = str(value)
    if len(text) > MAX_TEXT:
        text = text[:MAX_TEXT] + "… [text truncated]"
    return escape(text, quote=True)


def _number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        value = float(value)
    except (OverflowError, ValueError):
        return None
    return value if math.isfinite(value) and value >= 0 else None


def _duration(value):
    n = _number(value)
    return "Unknown" if n is None else f"{n:,.2f} ms"


def _sequence(value):
    return value if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)) else ()


def _mapping(value):
    return value if isinstance(value, Mapping) else {}


def _interval(event):
    start, end = _number(event.get("start_ms")), _number(event.get("end_ms"))
    if start is None or end is None or end < start:
        return None
    return (start, end)


def _inclusive(event):
    if "inclusive_ms" in event:
        return _number(event.get("inclusive_ms"))
    interval = _interval(event)
    return None if interval is None else interval[1] - interval[0]


def _library_exclusive(event):
    inclusive = _inclusive(event)
    exclusive = _number(event.get("exclusive_ms"))
    if inclusive is None or exclusive is None or exclusive > inclusive:
        return None
    return exclusive


def _union_length(intervals):
    total = 0.0
    end = None
    for start, stop in sorted(intervals):
        if end is None or start > end:
            total += stop - start
            end = stop
        elif stop > end:
            total += stop - end
            end = stop
    return total


def _exclusive(event, events, dropped):
    """Subtract the union of direct child intervals; uncertainty stays unknown."""
    interval = _interval(event)
    ident = event.get("span_id")
    if interval is None or ident is None or dropped is None or dropped != 0:
        return None
    if sum(1 for e in events if e.get("span_id") == ident) != 1:
        return None
    children = [e for e in events if e.get("parent_span_id") == ident]
    segments = []
    for child in children:
        child_interval = _interval(child)
        if child_interval is None:
            return None
        if child_interval[0] < interval[0] or child_interval[1] > interval[1]:
            return None
        start, end = child_interval
        if end > start:
            segments.append((start, end))
    return max(0.0, interval[1] - interval[0] - _union_length(segments))


def _dropped(report):
    value = report.get("dropped_events")
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _gap(report, events, dropped):
    """Uncovered wrapper wall time, only when all supplied intervals are complete."""
    wrapper = _number(report.get("wrapper_elapsed_ms"))
    if wrapper is None or dropped != 0 or not events:
        return None
    intervals = [_interval(e) for e in events]
    if any(i is None or i[1] > wrapper for i in intervals):
        return None
    return max(0.0, wrapper - _union_length(intervals))


def _metric(label, value):
    return f'<div class="metric"><span>{_text(label)}</span><strong>{value}</strong></div>'


def _table(rows):
    rows = _sequence(rows)
    if not rows:
        return '<p class="muted">No rows supplied.</p>'
    shown = rows[:MAX_ROWS]
    columns = []
    for row in shown:
        if isinstance(row, Mapping):
            for key in row:
                if key not in columns:
                    columns.append(key)
        elif "Value" not in columns:
            columns.append("Value")
    omitted_fields = max(0, len(columns) - MAX_FIELDS)
    columns = columns[:MAX_FIELDS]
    out = ['<div class="scroll"><table><thead><tr>']
    out.extend(f"<th scope=\"col\">{_text(c)}</th>" for c in columns)
    out.append("</tr></thead><tbody>")
    for row in shown:
        out.append("<tr>")
        for column in columns:
            value = row.get(column) if isinstance(row, Mapping) else (row if column == "Value" else None)
            out.append(f"<td>{_text(value)}</td>")
        out.append("</tr>")
    out.append("</tbody></table></div>")
    if len(rows) > MAX_ROWS or omitted_fields:
        out.append(f'<p class="muted">Display truncated: {max(0, len(rows) - MAX_ROWS)} rows and '
                   f'{omitted_fields} columns omitted.</p>')
    return "".join(out)


def _pairs(value):
    value = _mapping(value)
    if not value:
        return '<p class="muted">Not supplied.</p>'
    entries = list(value.items())
    out = ['<div class="scroll"><table><tbody>']
    for key, val in entries[:MAX_FIELDS]:
        out.append(f'<tr><th scope="row">{_text(key)}</th><td>{_text(val)}</td></tr>')
    out.append("</tbody></table></div>")
    if len(entries) > MAX_FIELDS:
        out.append(f'<p class="muted">{len(entries) - MAX_FIELDS} fields omitted.</p>')
    return "".join(out)


def _messages(report):
    out = []
    for key, title in (("notices", "Notices"), ("errors", "Errors")):
        messages = _sequence(report.get(key))
        if messages:
            out.append(f'<section><h2>{title}</h2><ul>')
            out.extend(f"<li>{_text(message)}</li>" for message in messages[:MAX_MESSAGES])
            out.append("</ul>")
            if len(messages) > MAX_MESSAGES:
                out.append(f'<p class="muted">{len(messages) - MAX_MESSAGES} messages omitted.</p>')
            out.append("</section>")
    return "".join(out)


_PHASE_DESCRIPTIONS = {
    "create_guard_discovery": "Analyzes source relations to discover upstream materialized views whose read guards must be acquired.",
    "create_catalog_lookup": "Looks up the materialized view in the OpenIVM catalog.",
    "create_mv_system_tables": "Prepares the OpenIVM catalog and change-tracking tables.",
    "create_analyze_query": "Analyzes the user's view query in Spark to resolve its shape and relations.",
    "create_resolve_sources": "Resolves the source relations used by the view.",
    "create_validate_sources": "Validates the resolved source relations.",
    "create_resolve_pin_binding": "Resolves any pinned source binding.",
    "create_collect_workload_facts": "Collects source constraint facts used for refresh planning.",
    "create_compile_classification": "Compiles the view query and determines its candidate refresh type.",
    "create_compile_failure": "Records a compiler failure and its supplied cause.",
    "create_classification_inputs": "Collects query-shape inputs used to classify refresh behavior.",
    "create_simple_projection_check": "Checks whether the simple-projection delta feed includes a data-table apply.",
    "create_resolve_upstream_mvs": "Resolves materialized views used as upstream sources.",
    "create_refresh_classification": "Records the compiled and effective refresh classifications.",
    "create_analyze_physical_schema": "Analyzes the initial-load query's output schema.",
    "create_capture_watermarks": "Records source change versions before initial load so earlier changes are not applied twice.",
    "create_resolve_direct_mv_dependency_map": "Finds direct upstream materialized-view dependencies.",
    "create_prepare_metadata": "Prepares catalog metadata for materialized-view creation.",
    "create_validate_catalog_registration": "Checks catalog registration before creating the materialized view.",
    "create_cleanup_stale_location": "Cleans stale files left at the materialized-view data location.",
    "create_probe_existing_delta_path": "Checks whether the materialized-view data location already contains a Delta table.",
    "create_mv_initial_load": "Initializes or recovers the materialized view's data.",
    "create_mv_catalog_registration": "Registers the materialized view and its backing table.",
    "create_mv_user_view": "Creates the public view over the backing table when needed.",
    "create_mv_publish_metadata": "Publishes materialized-view metadata after the initial load succeeds.",
    "metadata_pre_sql": "Resolves source metadata and pending changes before refresh SQL runs.",
    "metadata_post_sql": "Records cascade metadata and performs post-refresh cleanup.",
    "generate_refresh_sql.dispatch": "Selects the refresh path for the pending source changes.",
    "generate_refresh_sql.cdf_insert_only_probe": "Checks the CDF changes for an insert-only refresh.",
    "generate_refresh_sql.eligibility": "Checks whether the current changes qualify for the insert-only path.",
    "generate_refresh_sql.compiler_context": "Initializes the session-scoped compiler used to build refresh SQL.",
    "generate_refresh_sql.compile": "Compiles the materialized-view definition into a refresh program.",
    "generate_refresh_sql.assembly": "Rewrites the compiled program into Spark-executable statements.",
    "generate_refresh_sql.affected_join_prune": "Builds an affected-key query for the window join when the shape permits it.",
    "public_cdf_no_write_proof": "Checks whether public CDF proves that no materialized-view write is needed.",
    "pin_full_refresh_cascade_version": "Resolves the materialized-view version used by the full-refresh cascade.",
    "full_refresh_cascade_snapshot": "Writes the full-refresh cascade snapshot used to propagate changes downstream.",
    "execute_refresh_sql_attempt": "Runs a Spark SQL statement attempt; retries may appear as separate events.",
    "execute_refresh_sql_stmt": "Runs one generated Spark SQL statement.",
    "execute_refresh_cleanup_retry": "Runs incremental cleanup with Delta conflict retries.",
    "execute_full_refresh_retry": "Runs the full-refresh statement with Delta conflict retries.",
    "execute_full_refresh_attempt": "Runs one full-refresh statement attempt.",
    "execute_full_refresh_writer": "Runs the statement that recomputes the materialized-view result for a full refresh.",
    "finalize_mark_consumed": "Marks source changes as consumed by this refresh.",
    "finalize_consume_frozen_sources": "Marks the frozen source changes as consumed.",
    "finalize_data_version": "Reads the committed Delta version of the materialized-view data.",
    "finalize_catalog_advance": "Updates the catalog with the materialized view's new version.",
    "cdf_commit_classify": "Classifies source Delta commits since the recorded version.",
    "state_lag_check": "Checks whether restored state lags behind committed materialized-view data.",
    "state_backup": "Publishes a backup of OpenIVM state after the command.",
    "gc_prepare": "Identifies state eligible for cleanup after the refresh.",
    "state_barrier_wait": "Measures time waiting for the state synchronization barrier.",
    "state_barrier_hold": "Measures time holding the state synchronization barrier.",
    "sql_log_handoff": "Flushes the captured SQL log for the operation.",
    "profile_persist": "Persists the completed operation profile.",
    "spark_sql": "Submits the statement to Spark SQL.",
    "spark_collect": "Collects the Spark SQL result for this operation.",
    "window_running_state_load": "Loads cached running-window state from RocksDB.",
    "window_running_state_prepare": "Prepares the cached running-window state update.",
    "window_running_state_commit": "Commits running-window state at the new data version.",
    "window_running_branch_probe": "Checks which running-window branches are needed before mutating the target.",
    "original_query": "Captured definition text; this record is not a submitted SQL statement.",
    "register_source_delta": "Source-change representation; the query-log contract classifies it as diagnostic.",
    "rewritten_stmt": "Generated refresh statement; submission is verified only for submitted_sql records.",
    "count_monoid_cleanup": "Removes zero-multiplicity rows after aggregate maintenance.",
    "drop_cleanup": "Drops temporary change artifacts during cleanup.",
    "initial_load_ctas": "Creates the materialized-view data during initial load.",
    "catalog_registration": "Registers the materialized view in the catalog.",
    "backing_user_view": "Creates the view backing the user-facing materialized view.",
    "fused_view_delta_select": "Builds the view-delta relation used by dependent refreshes.",
    "post_cleanup_stage": "Stages cleanup work after the refresh statements.",
}


def _events(report, events, dropped):
    out = ['<section><h2>Phase waterfall</h2>']
    phases = [event for event in events if "sql" not in event and (
        event.get("span_id") is not None or _inclusive(event) is not None)]
    observations = [event for event in events if "sql" not in event and event not in phases]
    if not phases:
        out.append('<p class="muted">No measured spans available.</p>')
    else:
        out.append('<p class="muted">Offsets are relative to operation start. Inclusive time includes '
                   'children; exclusive time subtracts the union of direct child intervals.</p>')
        ends = [i[1] for i in (_interval(e) for e in phases) if i is not None]
        wrapper = _number(report.get("wrapper_elapsed_ms"))
        extent = max(ends + ([wrapper] if wrapper is not None else []), default=0)
        if extent:
            out.append(f'<p class="muted">Waterfall scale: 0 to {_duration(extent)}; '
                       'only completed measured intervals are drawn.</p>')
            out.append('<div class="scroll"><table class="phase-table"><thead><tr>'
                       '<th>Operation phase</th><th>Waterfall</th><th>Duration</th>'
                       '</tr></thead><tbody>')
            for event in phases[:MAX_SPANS]:
                interval = _interval(event)
                label = _phase_label(event.get("phase"))
                phase = event.get("phase")
                description = _PHASE_DESCRIPTIONS.get(phase) if isinstance(phase, str) else None
                description_html = (f'<span class="phase-description">{_text(description)}</span>'
                                    if description else "")
                bar = ""
                if interval is not None:
                    left = 100 * interval[0] / extent
                    width = 100 * (interval[1] - interval[0]) / extent
                    bar = (f'<div class="track" aria-label="Measured interval"><div class="bar" '
                           f'style="left:{left:.4f}%;width:{width:.4f}%"></div></div>')
                else:
                    bar = '<span class="muted">Interval unavailable</span>'
                out.append(f'<tr><td><span class="phase-name">{_text(label)}</span>{description_html}</td>'
                           f'<td>{bar}</td><td class="phase-duration">{_duration(_inclusive(event))}</td></tr>')
            out.append('</tbody></table></div>')
        exclusive = [(e, _exclusive(e, phases, dropped)) for e in phases]
        known = [(e, n) for e, n in exclusive if n is not None]
        if known and len(known) == len(phases):
            dominant, amount = max(known, key=lambda pair: pair[1])
            out.append(f'<p>Largest inferred exclusive span: {_text(dominant.get("phase"))} '
                       f'({_duration(amount)}). This is not a sum of parallel work.</p>')
        out.append('<details><summary>Individual span measurements</summary><div>')
        for event, amount in exclusive[:MAX_SPANS]:
            interval = _interval(event)
            inclusive = _inclusive(event)
            out.append(f'<details><summary>{_text(event.get("phase"))} '
                       f'· {_text(event.get("status"))} · inclusive {_duration(inclusive)}</summary><div>')
            if interval is not None and extent:
                left = 100 * interval[0] / extent
                width = 100 * (interval[1] - interval[0]) / extent
                out.append(f'<div class="track" aria-hidden="true"><div class="bar" '
                           f'style="left:{left:.4f}%;width:{width:.4f}%"></div></div>')
            out.append('<div class="grid">')
            out.append(_metric("Start offset", _duration(event.get("start_ms"))))
            out.append(_metric("End offset", _duration(event.get("end_ms"))))
            out.append(_metric("Library-provided exclusive", _duration(_library_exclusive(event))))
            out.append(_metric("Inferred exclusive wall time", _duration(amount)))
            out.append("</div>")
            out.append(f'<p class="muted">Span: {_text(event.get("span_id"))} · '
                       f'Parent: {_text(event.get("parent_span_id"))}</p>')
            out.append(f'<p class="muted">Library exclusive scope: {_text(event.get("exclusive_scope"))}. '
                       'Inferred time uses mapped child intervals, not the library measurement.</p>')
            if event.get("metrics"):
                out.append('<details><summary>Metrics</summary><div>' + _pairs(event.get("metrics")) + '</div></details>')
            out.append("</div></details>")
        out.append('</div></details>')
        if len(phases) > MAX_SPANS:
            out.append(f'<p class="muted">Display truncated: {len(phases) - MAX_SPANS} spans omitted.</p>')
    drop_text = "Unknown" if dropped is None else str(dropped)
    out.append(f'<p class="muted">Events dropped upstream: {drop_text}.</p>')
    if observations:
        out.append('<details><summary>Other recorded decisions and observations</summary><div>')
        for event in observations[:MAX_SPANS]:
            out.append(f'<p><strong>{_text(_phase_label(event.get("phase")))}</strong> · '
                       f'{_text(event.get("status"))}</p>' + _pairs(event.get("metrics")))
        if len(observations) > MAX_SPANS:
            out.append(f'<p class="muted">{len(observations) - MAX_SPANS} observations omitted.</p>')
        out.append('</div></details>')
    out.append("</section>")
    return "".join(out)


_SQL_KEYWORDS = frozenset("""
add all alter and anti as asc between by case cast create cross current_date current_timestamp
delete desc distinct drop else end except exists false fetch first following from full group having
in inner insert intersect into is join lateral left like limit matched materialized merge not null
nulls offset on or order outer over partition preceding range refresh replace right rows select semi
set source table target then true truncate union unmatched update using values view when where window
with
""".split())
_SQL_BREAKS = frozenset("""
select from where group having order limit union except intersect join on when else set values using
returning merge update delete insert with
""".split())
_SQL_DOLLAR_QUOTE = re.compile(r"\$(?:[A-Za-z_][A-Za-z_0-9]*)?\$")


def _sql_escape(value):
    return escape(value, quote=True).replace("\r", "&#13;")


def _sql_tokens(sql):
    """Keep quoted text and comments intact while identifying safe formatting boundaries."""
    i = 0
    while i < len(sql):
        start = i
        char = sql[i]
        if char.isspace():
            i += 1
            while i < len(sql) and sql[i].isspace():
                i += 1
            kind = "space"
        elif sql.startswith("--", i):
            i += 2
            while i < len(sql) and sql[i] not in "\r\n":
                i += 1
            kind = "comment"
        elif sql.startswith("/*", i):
            i += 2
            depth = 1
            while i < len(sql) and depth:
                if sql.startswith("/*", i):
                    depth += 1
                    i += 2
                elif sql.startswith("*/", i):
                    depth -= 1
                    i += 2
                else:
                    i += 1
            kind = "comment"
        elif char in "'\"`":
            i += 1
            while i < len(sql):
                if sql[i] == "\\" and i + 1 < len(sql):
                    i += 2
                elif sql[i] == char:
                    i += 1
                    if i < len(sql) and sql[i] == char:
                        i += 1
                    else:
                        break
                else:
                    i += 1
            kind = "identifier" if char == "`" else "string"
        elif char == "$" and (match := _SQL_DOLLAR_QUOTE.match(sql, i)):
            delimiter = match.group()
            end = sql.find(delimiter, i + len(delimiter))
            i = len(sql) if end < 0 else end + len(delimiter)
            kind = "string"
        elif char.isalpha() or char == "_":
            i += 1
            while i < len(sql) and (sql[i].isalnum() or sql[i] in "_$"):
                i += 1
            kind = "keyword" if sql[start:i].lower() in _SQL_KEYWORDS else "word"
        elif char.isdigit():
            i += 1
            while i < len(sql) and (sql[i].isdigit() or sql[i] == "."):
                i += 1
            kind = "number"
        else:
            i += 1
            kind = "punct"
        yield kind, sql[start:i]


def _formatted_sql(value):
    sql = "Unknown" if value is None else str(value)
    tokens = list(_sql_tokens(sql))
    out = []
    depth = 0
    previous = None
    for index, (kind, text) in enumerate(tokens):
        if kind == "space":
            next_token = tokens[index + 1] if index + 1 < len(tokens) else None
            if (next_token and next_token[0] == "keyword"
                    and next_token[1].lower() in _SQL_BREAKS
                    and previous is not None):
                text = "\n" + "  " * min(depth, 8)
            elif previous == "," and depth <= 1:
                text = "\n" + "  " * min(depth + 1, 8)
            out.append(_sql_escape(text))
            continue
        if previous == "," and depth <= 1 and index and tokens[index - 1][0] != "space":
            out.append("\n" + "  " * min(depth + 1, 8))
        escaped = _sql_escape(text)
        if kind in {"keyword", "string", "comment", "number", "identifier"}:
            out.append(f'<span class="sql-{kind}">{escaped}</span>')
        else:
            out.append(escaped)
        if kind == "punct":
            if text == "(":
                depth += 1
            elif text == ")":
                depth = max(0, depth - 1)
        previous = text if kind == "punct" else kind
    return "".join(out)


def _submitted_sql_display(value):
    raw = "Unknown" if value is None else str(value)
    return (f'<pre class="sql-code"><code>{_formatted_sql(value)}</code></pre>'
            '<details><summary>Raw captured SQL</summary><div>'
            f'<pre class="sql-code"><code>{_sql_escape(raw)}</code></pre>'
            '</div></details>')


def _executed_sql(events):
    records = [event for event in events if "sql" in event]
    out = ['<details><summary>Executed SQL</summary><div>']
    if not records:
        out.append('<p class="muted">No query-log records supplied.</p>')
    else:
        submitted = [event for event in records if event.get("status") == "submitted_sql"]
        other = [event for event in records if event.get("status") != "submitted_sql"]
        out.append('<h3>Submitted SQL records</h3>')
        out.append('<p class="muted">The query-log contract marks known SQL submission call sites as '
                   'submitted_sql. This does not assert that a statement succeeded. Phase telemetry and other '
                   'non-SQL work are not treated as SQL records.</p>')
        if not submitted:
            out.append('<p class="muted">No verified submitted_sql records supplied.</p>')
        for event in submitted[:MAX_SPANS]:
            metrics = _mapping(event.get("metrics"))
            out.append('<div class="sql-record"><strong>'
                       f'{_text(_phase_label(event.get("phase")))}</strong>'
                       f'<div class="sql-meta">submitted_sql · Order: {_text(metrics.get("stmt_order"))} · '
                       f'Attempt: {_text(metrics.get("attempt_idx"))} · '
                       f'Recorded duration: {_duration(metrics.get("duration_ms"))}</div>'
                       f'{_submitted_sql_display(event.get("sql"))}</div>')
        if len(submitted) > MAX_SPANS:
            out.append(f'<p class="muted">{len(submitted) - MAX_SPANS} submitted SQL records omitted.</p>')
        if other:
            out.append('<details><summary>Other captured representations — not verified as submitted SQL</summary><div>')
            out.append('<p class="muted">Original query text, plans, diagnostics, and unknown representations '
                       'are shown separately; SQL-looking text here is not evidence that Spark submitted it.</p>')
            for event in other[:MAX_SPANS]:
                metrics = _mapping(event.get("metrics"))
                kind = event.get("status")
                out.append('<div class="sql-record"><strong>'
                           f'{_text(_phase_label(event.get("phase")))}</strong>'
                           f'<div class="sql-meta">Representation: {_text(kind)} · '
                           f'Recorded duration: {_duration(metrics.get("duration_ms"))}</div>'
                           f'<pre>{_text(event.get("sql"))}</pre></div>')
            if len(other) > MAX_SPANS:
                out.append(f'<p class="muted">{len(other) - MAX_SPANS} other representations omitted.</p>')
            out.append('</div></details>')
    out.append('</div></details>')
    return "".join(out)


def _cdf_summary(report):
    changes = report.get("cdf_changes")
    if not isinstance(changes, Mapping):
        return ""
    counts = changes.get("changed_rows_by_type")
    if isinstance(counts, Mapping) and counts:
        counts_text = " · ".join(f"{_text(kind)}: {_text(value)}" for kind, value in counts.items())
    else:
        counts_text = "Not supplied"
    out = ['<div class="cdf-summary"><strong>Measured CDF changes</strong><div class="cdf-grid">']
    out.append(f'<div class="cdf-item">Source<strong>{_text(changes.get("source"))}</strong></div>')
    out.append('<div class="cdf-item">Version range<strong>'
               f'{_text(changes.get("starting_version"))} → {_text(changes.get("ending_version"))}'
               '</strong></div>')
    out.append(f'<div class="cdf-item">Changed rows by type<strong>{counts_text}</strong></div>')
    out.append('</div></div>')
    return "".join(out)


def _consumption_evidence(report):
    if "source_versions" not in report and "pending_delta_count" not in report:
        return ""
    out = ['<div class="consumption-evidence"><strong>OpenIVM consumption evidence</strong>']
    if "source_versions" in report:
        versions = report.get("source_versions")
        if isinstance(versions, Sequence) and not isinstance(versions, (str, bytes, bytearray)):
            if versions:
                for version in versions[:MAX_ROWS]:
                    version = _mapping(version)
                    relation = version.get("relation")
                    label = relation.rsplit(".", 1)[-1] if isinstance(relation, str) else relation
                    out.append('<div class="source-version">'
                               f'{_text(label)}: '
                               f'{_text(version.get("start_version"))} → '
                               f'{_text(version.get("end_version"))}</div>')
                if len(versions) > MAX_ROWS:
                    out.append(f'<p class="muted">{len(versions) - MAX_ROWS} source-version entries omitted.</p>')
            else:
                out.append('<p class="muted">No source-version entries recorded.</p>')
        else:
            out.append('<p class="muted">Source-version entries unavailable.</p>')
    if "pending_delta_count" in report:
        out.append(f'<div class="batch-count">Pending delta batches (not rows): '
                   f'{_text(report.get("pending_delta_count"))}</div>')
    out.append('</div>')
    return "".join(out)


def _outcome(report):
    value = report.get("library_outcome")
    if value is None:
        return ""
    classes = {
        "incremental_executed": "incremental",
        "full_refresh_executed": "full",
        "no_pending_deltas": "noop",
        "noop_fast_exit": "noop",
        "source_versions_already_applied": "noop",
        "runtime_empty_delta_skip": "noop",
    }
    labels = {
        "incremental_executed": "Incremental refresh executed",
        "full_refresh_executed": "Full refresh executed",
        "no_pending_deltas": "No-op outcome",
        "noop_fast_exit": "No-op outcome",
        "source_versions_already_applied": "No-op outcome",
        "runtime_empty_delta_skip": "No-op outcome",
    }
    style = classes.get(value, "other") if isinstance(value, str) else "other"
    label = labels.get(value, "Recorded library outcome") if isinstance(value, str) else "Recorded library outcome"
    out = [f'<div class="outcome outcome--{style}"><strong>{_text(label)}</strong>'
           f'<code>{_text(value)}</code>']
    if value == "full_refresh_executed":
        reasons = []
        for event in _sequence(report.get("events")):
            event = _mapping(event)
            metrics = _mapping(event.get("metrics"))
            if metrics.get("event_type") != "decision.completed":
                continue
            if event.get("phase") == "route":
                reasons.extend(_text(reason) for reason in _sequence(metrics.get("reasons"))
                               if reason is not None and str(reason).strip())
            elif event.get("phase") == "classification":
                reason = metrics.get("reason")
                if reason is not None and str(reason).strip():
                    reasons.append(_text(reason))
        if reasons:
            title = "Recorded decision reason" if len(reasons) == 1 else "Recorded decision reasons"
            out.append(f'<p class="outcome-reason">{title}: {" · ".join(reasons)}</p>')
    out.append('</div>')
    return "".join(out)


def _current_phase(events):
    running = [event for event in events if event.get("status") == "running"]
    out = ['<section><h2>Current phase</h2>']
    if running:
        event = running[-1]
        out.append(f'<p><span class="badge">Running</span> {_text(_phase_label(event.get("phase")))}</p>')
        out.append(f'<p class="muted">Span: {_text(event.get("span_id"))}. '
                   'Showing the last running event in the supplied snapshot.</p>')
    else:
        out.append('<p class="muted">No running phase supplied.</p>')
    out.append('<p class="muted">Updates automatically from measured events. Elapsed time is a heartbeat, not advancing phases. '
               'No completion percentage is inferred.</p></section>')
    return "".join(out)


def _render(report, live):
    report = _mapping(report)
    raw_events = _sequence(report.get("events"))
    events = [_mapping(event) for event in raw_events]
    dropped = _dropped(report)
    operation = report.get("operation_id") is not None or report.get("operation_name") is not None or bool(events)
    title = _text(report.get("operation_name")) if operation else "Awaiting operation"
    status = _text(report.get("status")) if operation else "Not started"
    out = [_STYLE, '<main class="lab"><header><div class="eyebrow">Fabric guided lab · ',
           "Live operation" if live else "Operation report", f'</div><h1>{title}</h1>',
           f'<span class="badge">Status: {status}</span>']
    if operation:
        out.append(f'<p class="muted">Operation ID: {_text(report.get("operation_id"))}</p>')
    else:
        out.append('<p>No operation has run.</p><p class="muted">Layout preview, no measured telemetry.</p>')
        out.append('</header></main>')
        return "".join(out)
    if report.get("status") == "Ready" and report.get("operation_id") is None and not events:
        out.append('<p>The runtime checks passed. No lab command has run yet.</p></header>')
        out.append('<details><summary>Verified runtime and storage</summary><div>'
                   + _pairs(report.get("provenance")) + '</div></details></main>')
        return "".join(out)
    if live:
        out.append(f'<div class="wall-duration"><span>Elapsed wall time</span>'
                   f'<strong>{_duration(report.get("wrapper_elapsed_ms"))}</strong></div>')
        out.append(_outcome(report))
        out.append(_cdf_summary(report))
        out.append(_consumption_evidence(report))
        out.append('</header>')
        out.append(_current_phase(events))
        out.append('<details><summary>Measurement notes</summary><div>' + _messages(report) + '</div></details></main>')
        return "".join(out)
    children = {event.get("parent_span_id") for event in events if event.get("parent_span_id") is not None}
    measured = [(event, _inclusive(event)) for event in events
                if event.get("span_id") not in children and _inclusive(event) is not None]
    if measured:
        peak, cost = max(measured, key=lambda pair: pair[1])
        out.append(f'<p><strong>Longest measured phase:</strong> {_text(_phase_label(peak.get("phase")))} '
                   f'({_duration(cost)}, inclusive). Nested and parallel times are not added.</p>')
    out.append(f'<div class="wall-duration"><span>Total wall duration</span>'
               f'<strong>{_duration(report.get("wrapper_elapsed_ms"))}</strong></div>')
    out.append(_outcome(report))
    out.append(_cdf_summary(report))
    out.append(_consumption_evidence(report))
    out.append('</header>')
    if "after_rows" in report:
        out.append('<section><h2>Result rows</h2>' + _table(report["after_rows"]) + '</section>')
    out.append('<details open><summary>Operation phases</summary>')
    out.append(_events(report, events, dropped))
    out.append('</details><details><summary>Execution details</summary>')
    out.append(_executed_sql(events))
    out.append('<section><h2>Measured time</h2><div class="grid">')
    out.append(_metric("Wrapper wall time", _duration(report.get("wrapper_elapsed_ms"))))
    out.append(_metric("Library inclusive time", _duration(report.get("library_elapsed_ms"))))
    out.append(_metric("Parallel task time (aggregate)", _duration(report.get("parallel_task_time_ms"))))
    spans = [event for event in events if "sql" not in event and (
        event.get("span_id") is not None or _inclusive(event) is not None)]
    out.append(_metric("Uninstrumented wrapper wall time", _duration(_gap(report, spans, dropped))))
    out.append('</div><p class="muted">Wrapper and library times are separate measurements; '
               'library time is inclusive, not a sum of spans. Parallel task time may exceed wall time. '
               'Uninstrumented time is the wrapper interval not covered by supplied spans; '
               'it is unknown if supplied timestamps are incomplete or dropped events are unknown or nonzero. '
               'It does not identify what ran in that interval.</p></section>')
    out.append('<section><h2>Data and provenance</h2>')
    out.append('<details><summary>Provenance</summary><div>' + _pairs(report.get("provenance")) + '</div></details>')
    for key, label in (("before_rows", "Before rows"), ("after_rows", "After rows")):
        out.append(f'<details><summary>{label}</summary><div>{_table(report.get(key))}</div></details>')
    out.append('</section>')
    out.append(_messages(report))
    out.append('</details>')
    out.append('</main>')
    return "".join(out)


def _phase_label(value):
    if value is None:
        return "Unidentified phase"
    if value == "finalize_mark_consumed":
        return "Marking source changes as applied"
    if value in ("incremental_executed", "full_refresh_executed", "no_pending_deltas",
                 "noop_fast_exit", "source_versions_already_applied", "runtime_empty_delta_skip"):
        return value.replace("_", " ")
    return str(value).replace("_", " ").replace(".", " / ")


def render_live(report):
    """Render a snapshot; call again with fresh measured values to update it."""
    return _render(report, True)


def render_report(report):
    """Render a final report using the same normalized adapter contract."""
    return _render(report, False)
