"""Darwin evaluator for the state-guard lanes.

Darwin loads this file from the trusted config directory (never from the candidate workspace),
runs the sibling `state_guard_eval.py` CLI against the candidate checkout and converts its
`@RESULT` / `@FEEDBACK` protocol into an EvaluationResult. Every failure becomes a zero-score
result with feedback; this evaluator never raises into Darwin.
"""

import asyncio
import importlib.util
import math
import os
import re
import signal
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
CLI = HERE / "state_guard_eval.py"
GRACE_S = 300.0
METRIC_LIMIT = 1.0e12
FEEDBACK_LIMIT = 12000
LOG_LIMIT = 3000
RESULT_LINE = re.compile(r"^@RESULT\s+([A-Za-z_]\w*)=(\S+)\s*$")
FEEDBACK_LINE = re.compile(r"^@FEEDBACK\s?(.*)$")
FAILURE_DEFAULTS = {"combined_score": 0.0, "wait_reduction": 0.0, "e2e_improvement_median": 0.0}

try:
    from darwin.configuration.providers import ProviderOptions, WithOptions
    from darwin.evaluators.base import BaseEvaluator
    from darwin.evaluators.model import EvaluationContext, EvaluationResult
except ImportError:  # Darwin is not installed (offline validation)
    import abc
    import dataclasses
    from typing import Any, Generic, TypeVar

    OptionsT = TypeVar("OptionsT")

    class ProviderOptions:  # type: ignore[no-redef]
        def __init__(self, **values: Any) -> None:
            fields: dict[str, Any] = {}
            for klass in reversed(type(self).__mro__):
                fields.update(getattr(klass, "__annotations__", {}))
            unknown = set(values) - set(fields)
            if unknown:
                raise ValueError(f"unknown options: {sorted(unknown)}")
            for name in fields:
                if name in values:
                    setattr(self, name, values[name])
                elif hasattr(type(self), name):
                    setattr(self, name, getattr(type(self), name))
                else:
                    raise ValueError(f"option {name!r} is required")

        @classmethod
        def model_validate(cls, data: dict[str, Any] | None) -> "ProviderOptions":
            return cls(**(data or {}))

    class WithOptions(Generic[OptionsT]):  # type: ignore[no-redef]
        def __init__(self, *, options: Any = None, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self.options = options

    class BaseEvaluator(abc.ABC):  # type: ignore[no-redef]
        def __init__(self, config: Any = None, database: Any = None, evaluator_config: Any = None, **kwargs: Any) -> None:
            self.config, self.database, self.evaluator_config = config, database, evaluator_config
            super().__init__(**kwargs)

        @abc.abstractmethod
        async def evaluate(self, context: Any) -> Any: ...

        async def run_in_container(self, commands: list[str], container: Any) -> str:
            return str(getattr(container, "cwd", ""))

    @dataclasses.dataclass
    class EvaluationContext:  # type: ignore[no-redef]
        program: Any = None
        container: Any = None
        run_id: Any = None

    @dataclasses.dataclass
    class EvaluationResult:  # type: ignore[no-redef]
        metrics: dict[str, float] = dataclasses.field(default_factory=dict)
        artifacts: dict[str, Any] = dataclasses.field(default_factory=dict)


def _load_redact():
    # Private module name: the daemon shares sys.modules across evaluators.
    spec = importlib.util.spec_from_file_location("_state_guard_runner", HERE / "runner.py")
    if spec is None or spec.loader is None:
        return lambda text: text
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module.redact


_redact = _load_redact()


def sane(value: float) -> float:
    if not math.isfinite(value):
        return 0.0
    return max(-METRIC_LIMIT, min(METRIC_LIMIT, value))


class StateGuardOptions(ProviderOptions):
    lane: str
    baseline_ref: str
    pairs: int = 3
    spark_target: str = "spark-3.5"
    baseline_repo: str = ""
    cache_dir: str = "~/.cache/openivm-darwin"
    run_timeout_s: float = 1200.0
    deadline_s: float = 5000.0
    manifest: str = ""
    lock_sha256: str = ""


def build_argv(options: StateGuardOptions, workspace: str) -> list[str]:
    argv = [
        sys.executable,
        str(CLI),
        "--mode",
        "candidate",
        "--workspace",
        workspace,
        "--lane",
        options.lane,
        "--baseline-ref",
        options.baseline_ref,
        "--pairs",
        str(options.pairs),
        "--spark-target",
        options.spark_target,
        "--cache-dir",
        options.cache_dir,
        "--run-timeout",
        str(options.run_timeout_s),
        "--deadline",
        str(options.deadline_s),
    ]
    for flag, value in (
        ("--baseline-repo", options.baseline_repo),
        ("--manifest", options.manifest),
        ("--lock-sha256", options.lock_sha256),
    ):
        if value:
            argv += [flag, value]
    return argv


async def _run_cli(argv: list[str], timeout: float) -> tuple[int | None, str, str]:
    """Run the CLI; returns (exit code or None on timeout, stdout, stderr)."""
    posix = os.name == "posix"
    proc = await asyncio.create_subprocess_exec(
        *argv,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=posix,
    )

    def kill() -> None:
        if proc.returncode is not None:
            return
        try:
            if posix:
                os.killpg(proc.pid, signal.SIGKILL)
            else:
                proc.kill()
        except (ProcessLookupError, PermissionError):
            pass

    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        kill()
        await proc.wait()
        return None, "", ""
    except BaseException:
        kill()
        raise
    return proc.returncode, out.decode("utf-8", "replace"), err.decode("utf-8", "replace")


def parse_output(stdout: str) -> tuple[dict[str, float], list[str]]:
    metrics: dict[str, float] = {}
    feedback: list[str] = []
    for line in stdout.splitlines():
        line = line.strip()
        if match := RESULT_LINE.match(line):
            try:
                metrics[match.group(1)] = sane(float(match.group(2)))
            except ValueError:
                metrics[match.group(1)] = 0.0
        elif match := FEEDBACK_LINE.match(line):
            feedback.append(_redact(match.group(1)))
    if any(value != 1.0 for name, value in metrics.items() if name.endswith("_gate")):
        metrics["combined_score"] = 0.0
    metrics.setdefault("combined_score", 0.0)
    return metrics, feedback


def failure_result(message: str, log: str = "") -> "EvaluationResult":
    artifacts = {"feedback": _redact(f"infra: {message}. This candidate gets combined_score 0.0 (no speed credit).")}
    if log.strip():
        artifacts["evaluator_log"] = _redact(log.strip()[-LOG_LIMIT:])
    return EvaluationResult(metrics={**FAILURE_DEFAULTS, "infra_error": 1.0}, artifacts=artifacts)


def result_from_cli(exit_code: int | None, stdout: str, stderr: str) -> "EvaluationResult":
    if exit_code is None:
        return failure_result("the evaluator CLI timed out and was killed", stderr)
    metrics, feedback = parse_output(stdout)
    if exit_code not in (0, 2) or not any(line.startswith("@RESULT") for line in stdout.splitlines()):
        return failure_result(f"the evaluator CLI exited {exit_code} without a result block", stderr or stdout)
    if exit_code == 2:
        metrics["combined_score"] = 0.0
        metrics["infra_error"] = 1.0
    else:
        metrics.setdefault("infra_error", 0.0)
    artifacts = {"feedback": "\n".join(feedback)[:FEEDBACK_LIMIT]}
    if exit_code == 2 and stderr.strip():
        artifacts["evaluator_log"] = _redact(stderr.strip()[-LOG_LIMIT:])
    return EvaluationResult(metrics=metrics, artifacts=artifacts)


class StateGuardEvaluator(BaseEvaluator, WithOptions[StateGuardOptions]):
    """Scores one candidate checkout for the lane named in the evaluator options."""

    async def evaluate(self, context: EvaluationContext) -> EvaluationResult:
        try:
            workspace = await self._workspace(context)
            argv = build_argv(self.options, workspace)
            exit_code, stdout, stderr = await _run_cli(argv, self.options.deadline_s + GRACE_S)
            return result_from_cli(exit_code, stdout, stderr)
        except Exception as exc:  # never crash Darwin; a broken evaluation scores 0.0
            return failure_result(f"evaluator wrapper error: {type(exc).__name__}: {exc}")

    async def _workspace(self, context: EvaluationContext) -> str:
        reported = (await self.run_in_container(["pwd"], context.container)).strip().splitlines()
        candidates = [reported[-1]] if reported else []
        candidates.append(str(getattr(context.container, "cwd", "") or ""))
        for path in candidates:
            if path and os.path.isdir(path):
                return path
        raise RuntimeError(f"candidate workspace not found (tried {candidates})")
