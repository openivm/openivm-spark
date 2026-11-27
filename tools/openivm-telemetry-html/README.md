# OpenIVM telemetry HTML

`openivm_telemetry_html` renders one OpenIVM CREATE or REFRESH operation as a
self-contained HTML fragment for a notebook cell. The fragment shows the
outcome, CDF and source-version evidence, a phase waterfall with inclusive and
inferred exclusive times, the submitted SQL, and provenance. The renderer uses
only the Python standard library, escapes every supplied value, and caps what it
displays (`MAX_SPANS`, `MAX_ROWS`, `MAX_FIELDS`, `MAX_MESSAGES`, `MAX_TEXT`).

The module is the renderer half of the Fabric guided-lab helper, unchanged. The
lab's Fabric glue (`notebookutils`, job and Lakehouse plumbing) stays in the
lab, which should import this package instead of keeping its own copy.

## Install

Build a wheel, then install it into a notebook environment, for example with
`%pip install` from a Lakehouse file or a Fabric environment's custom library:

```bash
npx --no-install nx run openivm-telemetry-html:package
# writes tools/openivm-telemetry-html/dist/openivm_telemetry_html-0.1.0-py3-none-any.whl
```

Inside a virtual environment, `pip install ./tools/openivm-telemetry-html` works
too. `show` needs IPython, which Jupyter and Fabric kernels already provide;
elsewhere, install the `ipython` extra.

## Render

```python
from openivm_telemetry_html import render_live, render_report

displayHTML(render_report(report))      # Fabric or Databricks notebooks
```

In any IPython kernel, `show` displays the fragment and returns a handle that
replaces the same output on the next call:

```python
from openivm_telemetry_html import show

handle = show(snapshot, live=True)      # in-progress snapshot
show(next_snapshot, live=True, handle=handle)
show(final_report, handle=handle)
```

## Input contract

The renderer does not read OpenIVM's telemetry objects directly. A caller-side
adapter maps measured values into one mapping. Missing values render as
`Unknown`, never as zero, and the adapter must not invent values.

| Report key                       | Meaning                                                                                               |
| -------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `operation_id`, `operation_name` | Identity shown in the header; without either, and without events, the page says no operation has run. |
| `status`                         | Status badge text.                                                                                    |
| `wrapper_elapsed_ms`             | Caller-measured wall time of the whole operation.                                                     |
| `library_elapsed_ms`             | Library-measured inclusive time, not a sum of spans.                                                  |
| `parallel_task_time_ms`          | Optional aggregate task time; it may exceed wall time.                                                |
| `events`                         | Sequence of event mappings (below).                                                                   |
| `dropped_events`                 | Non-negative count of events lost upstream; inferred times need it to be `0`.                         |
| `library_outcome`                | Native outcome, such as `incremental_executed`, `full_refresh_executed` or `no_pending_deltas`.       |
| `cdf_changes`                    | Optional `source`, `starting_version`, `ending_version` and `changed_rows_by_type`.                   |
| `source_versions`                | Optional sequence of `relation`, `start_version` and `end_version`.                                   |
| `pending_delta_count`            | Optional count of pending delta batches, not rows.                                                    |
| `provenance`                     | Mapping of runtime and storage facts.                                                                 |
| `before_rows`, `after_rows`      | Sequences of mappings or values.                                                                      |
| `notices`, `errors`              | Sequences of text.                                                                                    |

| Event key                                         | Meaning                                                                                                                           |
| ------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `phase`                                           | Phase name, such as `execute_refresh_sql_stmt`; known phases get a description.                                                   |
| `span_id`, `parent_span_id`                       | Span tree identity.                                                                                                               |
| `status`                                          | `running`, a completion status, or for SQL records the representation, such as `submitted_sql`.                                   |
| `start_ms`, `end_ms`                              | Finite, non-negative offsets from the operation start on the wrapper clock; `end_ms` may be absent while running.                 |
| `inclusive_ms`, `exclusive_ms`, `exclusive_scope` | Optional library-measured durations and what the exclusive value covers.                                                          |
| `metrics`                                         | Mapping. SQL records use `stmt_order`, `attempt_idx` and `duration_ms`; decision events use `event_type`, `reasons` and `reason`. |
| `sql`                                             | Present only on query-log records; only `status=submitted_sql` is shown as submitted SQL.                                         |

Inferred exclusive time subtracts the union of direct child intervals.
Uninstrumented time is the wrapper interval that no span covers. Both are shown
only when every interval is complete and `dropped_events` is `0`.
[SQL log export](../../spark-ext/docs/QUERY_LOG_EXPORT.md) describes the query-log
categories behind `status`.

## Test

```bash
npx --no-install nx run openivm-telemetry-html:test
```
