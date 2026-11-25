"""Candidate diff policy: only allowlisted product code may change relative to the baseline."""

import json
import os
import re
from pathlib import Path
from typing import Any

from runner import InfraError, run_git

DEFAULT_MANIFEST = Path(__file__).resolve().parent.parent / "manifest.json"
SYMLINK_MODE = "120000"
REGULAR_MODE = "100644"
UNTRACKED_PATHSPECS = ("spark-ext/*/src/*",)


class PolicyError(InfraError):
    pass


def load_manifest(path: Any = None) -> dict[str, Any]:
    with open(path or DEFAULT_MANIFEST, "r", encoding="utf-8") as handle:
        return json.load(handle)


def glob_to_regex(pattern: str, ignore_case: bool = False) -> "re.Pattern[str]":
    out: list[str] = []
    i = 0
    while i < len(pattern):
        if pattern.startswith("**/", i):
            out.append("(?:.*/)?")
            i += 3
        elif pattern.startswith("**", i):
            out.append(".*")
            i += 2
        elif pattern[i] == "*":
            out.append("[^/]*")
            i += 1
        elif pattern[i] == "?":
            out.append("[^/]")
            i += 1
        else:
            out.append(re.escape(pattern[i]))
            i += 1
    return re.compile("".join(out) + r"\Z", re.IGNORECASE if ignore_case else 0)


def _normalize(path: str) -> str:
    path = path.replace("\\", "/")
    while path.startswith("./"):
        path = path[2:]
    return path


def _unsafe(path: str) -> bool:
    return path.startswith("/") or re.match(r"^[A-Za-z]:", path) is not None or ".." in path.split("/")


def is_frozen(path: str, manifest: dict[str, Any]) -> bool:
    frozen = manifest["frozen"]
    if any(glob_to_regex(g, True).match(path) for g in frozen["globs"]):
        return True
    basenames = [glob_to_regex(g, True) for g in frozen["basename_globs"]]
    return any(rx.match(segment) for segment in path.split("/") for rx in basenames)


def is_protocol_file(path: str, manifest: dict[str, Any]) -> bool:
    return any(glob_to_regex(g).match(path) for g in manifest["protocol_files"])


def lane_owners(path: str, manifest: dict[str, Any]) -> list[str]:
    return [
        lane
        for lane, spec in sorted(manifest["lanes"].items())
        if path in spec["owned"] or path in spec["shared"]
    ]


def check_changes(changes: list[dict[str, Any]], manifest: dict[str, Any], lane: str) -> dict[str, Any]:
    spec = manifest["lanes"][lane]
    owned, shared = set(spec["owned"]), set(spec["shared"])
    new_file_rx = [glob_to_regex(p) for p in spec["new_files"]]
    violations: list[str] = []
    warnings: list[str] = []
    changed: list[str] = []
    shared_touched = 0
    for change in sorted(changes, key=lambda c: c["path"]):
        path = _normalize(change["path"])
        status = str(change["status"])[:1].upper()
        old_mode = str(change.get("old_mode") or "")
        new_mode = str(change.get("new_mode") or "")
        reasons: list[str] = []
        if _unsafe(path):
            reasons.append("unsafe path")
        if status == "D":
            reasons.append("deleted")
        if SYMLINK_MODE in (old_mode, new_mode):
            reasons.append("symlink")
        if change.get("binary"):
            reasons.append("binary blob")
        if status == "M" and old_mode and new_mode and old_mode != new_mode:
            reasons.append(f"mode change {old_mode}->{new_mode}")
        if status == "A" and new_mode and new_mode != REGULAR_MODE and SYMLINK_MODE != new_mode:
            reasons.append(f"non-regular file mode {new_mode}")
        if status not in ("A", "M", "D"):
            reasons.append(f"unsupported change type {status}")
        if reasons:
            violations.append(f"{path}: {', '.join(reasons)}")
            continue
        if is_protocol_file(path, manifest):
            continue
        changed.append(path)
        if is_frozen(path, manifest):
            violations.append(f"{path}: frozen (tests, build, harness, telemetry and CI files cannot change)")
        elif status == "A":
            if not any(rx.match(path) for rx in new_file_rx):
                violations.append(f"{path}: new file outside lane {lane.upper()} allowlist")
        elif path not in owned and path not in shared:
            violations.append(f"{path}: not in lane {lane.upper()} allowlist")
        elif path in shared:
            shared_touched += 1
            others = [l.upper() for l in lane_owners(path, manifest) if l != lane]
            warnings.append(f"shared file {path} touched; also used by lane(s) {', '.join(others) or 'none'}; integrator must merge")
    return {
        "ok": not violations,
        "violations": violations,
        "warnings": warnings,
        "changed": changed,
        "shared_touched": shared_touched,
    }


def resolve_baseline(workspace: Any, ref: str, baseline_repo: str | None = None) -> str:
    rc, out = run_git(workspace, "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}")
    if rc == 0:
        return out.decode().strip()
    rc, origin = run_git(workspace, "remote", "get-url", "origin")
    sources = [s for s in (baseline_repo, str(workspace), origin.decode().strip() if rc == 0 else "") if s]
    for source in sources:
        if run_git(workspace, "fetch", "--depth=1", "--no-tags", source, ref)[0] != 0:
            continue
        rc, out = run_git(workspace, "rev-parse", "--verify", "FETCH_HEAD^{commit}")
        if rc == 0:
            return out.decode().strip()
    raise PolicyError(f"cannot resolve baseline ref {ref!r} from {sources}")


def _split_nul(data: bytes) -> list[str]:
    return [token.decode("utf-8", "surrogateescape") for token in data.split(b"\0") if token]


def _looks_binary(path: Path) -> bool:
    try:
        with open(path, "rb") as handle:
            return b"\0" in handle.read(8192)
    except OSError:
        return False


def collect_changes(workspace: Any, base_sha: str) -> list[dict[str, Any]]:
    """Tracked diff vs the baseline plus untracked files, including ignored files under src/."""
    root = Path(workspace)
    rc, raw = run_git(root, "diff", "--raw", "-z", "--no-renames", "--no-abbrev", base_sha, "--", ".")
    if rc != 0:
        raise PolicyError(f"git diff against {base_sha} failed")
    rc, numstat = run_git(root, "diff", "--numstat", "-z", "--no-renames", base_sha, "--", ".")
    if rc != 0:
        raise PolicyError("git diff --numstat failed")
    binary = set()
    for token in _split_nul(numstat):
        added, _, rest = token.partition("\t")
        deleted, _, path = rest.partition("\t")
        if added == "-" and deleted == "-":
            binary.add(path)
    tokens = _split_nul(raw)
    changes: dict[str, dict[str, Any]] = {}
    for meta, path in zip(tokens[0::2], tokens[1::2]):
        fields = meta.lstrip(":").split(" ")
        changes[path] = {
            "path": path,
            "status": fields[4],
            "old_mode": fields[0],
            "new_mode": fields[1],
            "binary": path in binary,
        }
    untracked_args = [
        ("ls-files", "--others", "--exclude-standard", "-z"),
        ("ls-files", "--others", "--ignored", "--exclude-standard", "-z", "--", *UNTRACKED_PATHSPECS),
    ]
    for args in untracked_args:
        rc, out = run_git(root, *args)
        if rc != 0:
            raise PolicyError("git ls-files failed")
        for path in _split_nul(out):
            full = root / path
            if os.path.islink(full):
                mode = SYMLINK_MODE
            else:
                mode = "100755" if os.name == "posix" and os.access(full, os.X_OK) else REGULAR_MODE
            changes[path] = {
                "path": path,
                "status": "A",
                "old_mode": "000000",
                "new_mode": mode,
                "binary": mode != SYMLINK_MODE and _looks_binary(full),
            }
    return list(changes.values())
