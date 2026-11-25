"""Run the trusted state-guard suite (Nx in the devcontainer) or replay fixture reports.

This module has no sibling imports at module level so the Darwin wrapper can load it
under a private name without clashing with other modules in the daemon.
"""

import collections
import dataclasses
import os
import re
import shlex
import shutil
import signal
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Callable, Protocol

SECRET_PATTERNS = (
    (re.compile(r"gh[pousr]_[A-Za-z0-9]{20,}"), "[REDACTED]"),
    (re.compile(r"github_pat_[A-Za-z0-9_]{20,}"), "[REDACTED]"),
    (re.compile(r"(?i)\bBearer\s+\S+"), "Bearer [REDACTED]"),
    (re.compile(r"(?i)(https?://)[^/\s:@]+:[^/\s@]+@"), r"\1[REDACTED]@"),
    (re.compile(r"(?i)\b([A-Za-z0-9_]*(?:token|password|passwd|secret))\s*([=:])\s*\S+"), r"\1\2[REDACTED]"),
)
SECRET_ENV = re.compile(r"(?i)(TOKEN|SECRET|PASSWORD|CREDENTIAL|API_?KEY)")
FAILURE_LINE = re.compile(r"\*\*\* FAILED|\[error\]|Exception|AssertionError|error:", re.IGNORECASE)
TAIL_LINES = 400
MAX_FAILURE_LINES = 60


class InfraError(Exception):
    """The evaluator itself (not the candidate) could not do its job."""


def redact(text: str) -> str:
    for pattern, replacement in SECRET_PATTERNS:
        text = pattern.sub(replacement, text)
    return text


def run_git(cwd: Any, *args: str, timeout: float = 600.0) -> tuple[int, bytes]:
    try:
        completed = subprocess.run(
            ["git", "-c", "core.quotepath=off", *args],
            cwd=str(cwd),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise InfraError(f"git {' '.join(args[:2])} failed to run: {exc}") from exc
    return completed.returncode, completed.stdout


@dataclasses.dataclass
class RunResult:
    exit_code: int | None
    timed_out: bool = False
    duration_s: float = 0.0
    perf: Any = None
    crash: Any = None
    read_errors: list[str] = dataclasses.field(default_factory=list)
    log_tail: list[str] = dataclasses.field(default_factory=list)
    failure_lines: list[str] = dataclasses.field(default_factory=list)

    def failure_excerpt(self, limit: int = 12) -> list[str]:
        lines = self.failure_lines[:limit] or self.log_tail[-15:]
        return [redact(line.rstrip()) for line in lines]


class Runner(Protocol):
    def run(self, role: str, root: Path, label: str, spark_target: str, timeout_s: float) -> RunResult: ...


def _read_reports(report_dir: Path, manifest: dict[str, Any], result: RunResult) -> None:
    import report  # lazy: only the CLI path needs it

    for attr, name in (("perf", manifest["nx"]["perf_report"]), ("crash", manifest["nx"]["crash_report"])):
        path = report_dir / name
        if not path.is_file():
            result.read_errors.append(f"{name}: not written")
            continue
        try:
            setattr(result, attr, report.load_json(path))
        except (OSError, ValueError) as exc:
            result.read_errors.append(f"{name}: unreadable ({exc})")


def scrubbed_env() -> dict[str, str]:
    return {k: v for k, v in os.environ.items() if not SECRET_ENV.search(k)}


class NxRunner:
    """Runs `nx run spark-ext:state-guard-perf` for one repo root and collects its reports."""

    def __init__(self, manifest: dict[str, Any], exec_prefix: str | None = None, container_root: str | None = None) -> None:
        self.manifest = manifest
        prefix = os.environ.get("OPENIVM_DARWIN_EXEC_PREFIX", "") if exec_prefix is None else exec_prefix
        self.prefix = shlex.split(prefix)
        self.container_root = container_root or os.environ.get(
            "OPENIVM_DARWIN_CONTAINER_ROOT", "/workspaces/openivm-spark"
        )

    def command(self, spark_target: str, report_dir: str, label: str) -> list[str]:
        nx = self.manifest["nx"]
        base = ["npx", "--no-install", "nx", "run", f"{nx['project']}:{nx['target']}", f"--configuration={spark_target}"]
        if not self.prefix:
            return base
        exports = [f"{nx['report_dir_env']}={report_dir}", f"{nx['run_label_env']}={label}"]
        return [*self.prefix, "env", *exports, *base]

    def run(self, role: str, root: Path, label: str, spark_target: str, timeout_s: float) -> RunResult:
        nx = self.manifest["nx"]
        relative = Path(".logs") / "state-guard" / label
        report_dir = Path(root) / relative
        shutil.rmtree(report_dir, ignore_errors=True)
        report_dir.mkdir(parents=True, exist_ok=True)
        in_container = f"{self.container_root.rstrip('/')}/{relative.as_posix()}" if self.prefix else str(report_dir)
        env = scrubbed_env()
        env[nx["report_dir_env"]] = in_container
        env[nx["run_label_env"]] = label
        log_path = report_dir.parent / f"{label}.run.log"
        result = RunResult(exit_code=None)
        started = time.monotonic()
        tail: collections.deque[str] = collections.deque(maxlen=TAIL_LINES)
        try:
            proc = subprocess.Popen(
                self.command(spark_target, in_container, label),
                cwd=str(root),
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                errors="replace",
                bufsize=1,
                start_new_session=os.name == "posix",
            )
        except OSError as exc:
            raise InfraError(f"cannot start the state-guard suite: {exc}") from exc

        def pump() -> None:
            with open(log_path, "w", encoding="utf-8") as log:
                assert proc.stdout is not None
                for raw in proc.stdout:
                    line = redact(raw.rstrip("\n"))
                    log.write(line + "\n")
                    tail.append(line)
                    if FAILURE_LINE.search(line) and len(result.failure_lines) < MAX_FAILURE_LINES:
                        result.failure_lines.append(line)

        reader = threading.Thread(target=pump, daemon=True)
        reader.start()
        try:
            result.exit_code = proc.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            result.timed_out = True
            _kill_tree(proc)
        reader.join(timeout=10)
        if not reader.is_alive() and proc.stdout is not None:
            proc.stdout.close()
        result.duration_s = time.monotonic() - started
        result.log_tail = list(tail)
        if not result.timed_out:
            _read_reports(report_dir, self.manifest, result)
        return result


def _kill_tree(proc: "subprocess.Popen[str]") -> None:
    try:
        if os.name == "posix":
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        else:
            proc.kill()
    except (OSError, ProcessLookupError):
        pass
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        pass


class FixtureRunner:
    """Replays `<dir>/<set>/state-guard-*.json` instead of invoking Nx (offline validation)."""

    def __init__(self, directory: Path, manifest: dict[str, Any], baseline_set: str, candidate_set: str) -> None:
        self.directory = Path(directory)
        self.manifest = manifest
        self.sets = {"baseline": baseline_set, "candidate": candidate_set}

    def run(self, role: str, root: Path, label: str, spark_target: str, timeout_s: float) -> RunResult:
        import report

        folder = self.directory / self.sets[role]
        if not folder.is_dir():
            raise InfraError(f"fixture set {folder} does not exist")
        result = RunResult(exit_code=0)
        meta = folder / "run.json"
        if meta.is_file():
            info = report.load_json(meta)
            result.exit_code = int(info.get("exit_code", 0))
            result.log_tail = [str(line) for line in info.get("log", [])]
            result.failure_lines = [line for line in result.log_tail if FAILURE_LINE.search(line)]
            result.timed_out = bool(info.get("timed_out", False))
        _read_reports(folder, self.manifest, result)
        return result


def prepare_baseline_worktree(cache_dir: Path, sha: str, sources: list[str]) -> Path:
    """A standalone checkout of `sha`; reused across pairs so its build outputs persist."""
    path = Path(cache_dir) / "baseline" / sha[:12]
    path.mkdir(parents=True, exist_ok=True)
    if not (path / ".git").exists():
        _must_git(path, "init", "--quiet")
    rc, head = run_git(path, "rev-parse", "--verify", "--quiet", "HEAD^{commit}")
    if rc != 0 or head.decode().strip() != sha:
        for source in sources:
            if run_git(path, "fetch", "--depth=1", "--no-tags", source, sha)[0] == 0:
                break
        else:
            raise InfraError(f"cannot fetch baseline {sha} from any of {sources}")
        _must_git(path, "checkout", "--force", "--detach", "FETCH_HEAD")
    else:
        _must_git(path, "reset", "--hard", sha)
    _must_git(path, "clean", "-fd")
    return path


def _must_git(cwd: Path, *args: str) -> None:
    rc, _ = run_git(cwd, *args)
    if rc != 0:
        raise InfraError(f"git {' '.join(args)} failed in {cwd}")


class BoxLock:
    """Serializes timing runs on one machine; a stale lock (crashed holder) is stolen."""

    def __init__(self, path: Path, wait_s: float, stale_s: float = 10800.0, sleep: Callable[[float], None] = time.sleep) -> None:
        self.path = Path(path)
        self.wait_s = wait_s
        self.stale_s = stale_s
        self.sleep = sleep

    def __enter__(self) -> "BoxLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + self.wait_s
        while True:
            try:
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                try:
                    if time.time() - self.path.stat().st_mtime > self.stale_s:
                        self.path.unlink()
                        continue
                except OSError:
                    continue
                if time.monotonic() >= deadline:
                    raise InfraError(f"timing box is busy ({self.path})")
                self.sleep(5.0)
                continue
            with os.fdopen(fd, "w") as handle:
                handle.write(f"pid={os.getpid()}\n")
            return self

    def __exit__(self, *exc: Any) -> None:
        try:
            self.path.unlink()
        except OSError:
            pass
