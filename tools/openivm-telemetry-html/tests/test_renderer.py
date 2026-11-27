import subprocess
import sys
import unittest
from pathlib import Path

from openivm_telemetry_html import render_live, render_report
from openivm_telemetry_html.renderer import MAX_SPANS, MAX_TEXT

PROJECT_ROOT = Path(__file__).resolve().parents[1]
HOSTILE = '<script>alert("x")</script>'


def span(span_id, start, end, parent=None, phase="execute_refresh_sql_stmt"):
    return {"phase": phase, "span_id": span_id, "parent_span_id": parent, "status": "ok",
            "start_ms": start, "end_ms": end}


def sql_record(sql, **metrics):
    return {"phase": "rewritten_stmt", "status": "submitted_sql", "sql": sql, "metrics": metrics}


class EscapingTests(unittest.TestCase):
    def test_every_supplied_value_is_escaped(self):
        report = {
            "operation_id": HOSTILE, "operation_name": HOSTILE, "status": HOSTILE,
            "events": [span("a", 0, 10, phase=HOSTILE), sql_record(f"SELECT '{HOSTILE}' FROM t", stmt_order=HOSTILE)],
            "provenance": {HOSTILE: HOSTILE}, "after_rows": [{HOSTILE: HOSTILE}], "errors": [HOSTILE],
            "dropped_events": 0,
        }
        for html in (render_report(report), render_live(report)):
            self.assertNotIn("<script>", html)
            self.assertIn("&lt;script&gt;", html)

    def test_sql_formatting_keeps_literals_and_comments_intact(self):
        html = render_report({"operation_id": "op", "events": [
            sql_record("SELECT 'a from b' AS x FROM t -- where y\nWHERE z = 1")]})
        self.assertIn('<span class="sql-string">&#x27;a from b&#x27;</span>', html)
        self.assertIn('<span class="sql-comment">-- where y</span>', html)
        self.assertIn('x\n<span class="sql-keyword">FROM</span>', html)


class TimingTests(unittest.TestCase):
    @staticmethod
    def nested(dropped):
        return {"operation_id": "op", "wrapper_elapsed_ms": 120, "dropped_events": dropped,
                "events": [span("p", 0, 100), span("c1", 10, 40, "p"), span("c2", 30, 60, "p")]}

    def test_inferred_times_subtract_interval_unions(self):
        html = render_report(self.nested(0))
        self.assertIn("Inferred exclusive wall time</span><strong>50.00 ms", html)
        self.assertIn("Uninstrumented wrapper wall time</span><strong>20.00 ms", html)

    def test_unknown_dropped_events_make_inferred_times_unknown(self):
        html = render_report(self.nested(None))
        self.assertIn("Inferred exclusive wall time</span><strong>Unknown", html)
        self.assertIn("Uninstrumented wrapper wall time</span><strong>Unknown", html)
        self.assertNotIn("50.00 ms", html)

    def test_invalid_durations_render_unknown_not_zero(self):
        html = render_report({"operation_id": "op", "wrapper_elapsed_ms": float("nan"),
                              "library_elapsed_ms": True, "parallel_task_time_ms": -1})
        for label in ("Wrapper wall time", "Library inclusive time", "Parallel task time (aggregate)"):
            self.assertIn(f"{label}</span><strong>Unknown", html)


class ContentTests(unittest.TestCase):
    def test_text_and_span_lists_are_capped(self):
        events = [span(f"s{i}", i, i + 1) for i in range(MAX_SPANS + 5)]
        html = render_report({"operation_id": "op", "operation_name": "x" * (MAX_TEXT + 50),
                              "events": events, "dropped_events": 0})
        self.assertIn("x" * MAX_TEXT + "… [text truncated]", html)
        self.assertNotIn("x" * (MAX_TEXT + 1), html)
        self.assertIn("Display truncated: 5 spans omitted.", html)
        self.assertNotIn(f"Span: s{MAX_SPANS} ", html)

    def test_full_refresh_shows_recorded_decision_reasons(self):
        html = render_report({"operation_id": "op", "library_outcome": "full_refresh_executed", "events": [
            {"phase": "route", "metrics": {"event_type": "decision.completed", "reasons": ["window_shape", " "]}},
            {"phase": "classification", "metrics": {"event_type": "decision.completed", "reason": "no_cdf"}}]})
        self.assertIn("Recorded decision reasons: window_shape · no_cdf", html)

    def test_live_snapshot_shows_running_phase_instead_of_report_sections(self):
        report = {"operation_id": "op", "wrapper_elapsed_ms": 5, "events": [
            span("a", 0, 2, phase="metadata_pre_sql"),
            {"phase": "execute_refresh_sql_stmt", "span_id": "b", "status": "running", "start_ms": 2}]}
        live = render_live(report)
        self.assertIn('<span class="badge">Running</span> execute refresh sql stmt', live)
        self.assertNotIn("Phase waterfall", live)
        final = render_report(report)
        self.assertIn("Phase waterfall", final)
        self.assertNotIn("Current phase", final)
        self.assertIn("No operation has run.", render_live({}))


class ImportTests(unittest.TestCase):
    def test_import_needs_no_notebook_runtime(self):
        code = ("import sys, openivm_telemetry_html; "
                "sys.exit(1 if {'IPython', 'notebookutils'} & set(sys.modules) else 0)")
        result = subprocess.run([sys.executable, "-B", "-c", code], cwd=PROJECT_ROOT,
                                capture_output=True, text=True)
        self.assertEqual(0, result.returncode, result.stderr)


if __name__ == "__main__":
    unittest.main()
