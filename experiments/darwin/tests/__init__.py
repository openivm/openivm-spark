"""Offline validation of the state-guard evaluator (stdlib unittest, no Darwin / Nx / Spark)."""

import contextlib
import copy
import shutil
import sys
import tempfile
from pathlib import Path

DARWIN = Path(__file__).resolve().parent.parent
EVALUATOR = DARWIN / "evaluator"
FIXTURES = DARWIN / "fixtures"
SCRATCH = DARWIN / ".scratch"

if str(EVALUATOR) not in sys.path:
    sys.path.insert(0, str(EVALUATOR))

import policy  # noqa: E402
import report  # noqa: E402

MANIFEST = policy.load_manifest()


def perf(report_set: str) -> dict:
    return copy.deepcopy(report.load_json(FIXTURES / "reports" / report_set / "state-guard-perf.json"))


def crash(report_set: str) -> dict:
    return copy.deepcopy(report.load_json(FIXTURES / "reports" / report_set / "state-guard-crash.json"))


@contextlib.contextmanager
def scratch_dir():
    """Scratch space inside the checkout (never the system temp dir), removed afterwards."""
    SCRATCH.mkdir(exist_ok=True)
    path = Path(tempfile.mkdtemp(dir=SCRATCH))
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)
        with contextlib.suppress(OSError):
            SCRATCH.rmdir()
