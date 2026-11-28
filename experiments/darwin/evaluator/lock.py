"""Integrity lock for the trusted evaluator files (line-ending independent sha256)."""

import argparse
import hashlib
import json
import sys
from pathlib import Path

LOCK_NAME = "evaluator.lock.json"
LOCK_SCHEMA = "openivm.darwin-lock/v1"
ROOT = Path(__file__).resolve().parent.parent


def file_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def tracked(root: Path) -> list[str]:
    files = [p.relative_to(root).as_posix() for p in (root / "evaluator").glob("*.py")]
    return sorted(files) + ["manifest.json"]


def build(root: Path) -> dict:
    return {"schema": LOCK_SCHEMA, "files": {rel: file_digest(root / rel) for rel in tracked(root)}}


def render(lock: dict) -> str:
    return json.dumps(lock, indent=2, sort_keys=True) + "\n"


def lock_digest(root: Path) -> str:
    return file_digest(root / LOCK_NAME)


def verify(root: Path = ROOT, expected_lock_digest: str = "") -> list[str]:
    lock_path = root / LOCK_NAME
    if not lock_path.is_file():
        return [f"{LOCK_NAME} is missing"]
    errors: list[str] = []
    if expected_lock_digest and lock_digest(root) != expected_lock_digest.strip().lower():
        errors.append("lock file digest does not match the pinned digest")
    try:
        recorded = json.loads(lock_path.read_text(encoding="utf-8"))["files"]
    except (ValueError, KeyError, TypeError) as exc:
        return errors + [f"{LOCK_NAME} is unreadable: {exc}"]
    expected = tracked(root)
    for rel in sorted(set(recorded) | set(expected)):
        path = root / rel
        if rel not in recorded:
            errors.append(f"{rel}: not in lock")
        elif not path.is_file():
            errors.append(f"{rel}: missing")
        elif file_digest(path) != recorded[rel]:
            errors.append(f"{rel}: digest mismatch")
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--write", action="store_true", help="regenerate the lock file")
    group.add_argument("--verify", action="store_true", help="verify files against the lock")
    group.add_argument("--digest", action="store_true", help="print the sha256 of the lock file")
    args = parser.parse_args(argv)
    if args.write:
        (ROOT / LOCK_NAME).write_text(render(build(ROOT)), encoding="utf-8", newline="\n")
        return 0
    if args.digest:
        print(lock_digest(ROOT))
        return 0
    errors = verify(ROOT)
    for error in errors:
        print(error, file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
