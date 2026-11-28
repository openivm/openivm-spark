#!/usr/bin/env bash
set -euo pipefail

python_target="${1:?expected spark-3.5 or spark-4.1}"
case "$python_target" in
    spark-3.5|spark-4.1) ;;
    *) echo "[python-env] FATAL: unsupported target '$python_target'" >&2; exit 1 ;;
esac
python_dir="/opt/openivm-python/$python_target"
python_requirements="$(dirname "${BASH_SOURCE[0]}")/requirements-py4j-$python_target.txt"
python_version="$(sed -n 's/^py4j==\([0-9.]*\) .*/\1/p' "$python_requirements")"
export PYTHONPATH="$python_dir${PYTHONPATH:+:$PYTHONPATH}"
python3 - "$python_dir" "$python_version" <<'PY'
import pathlib
import sys
import py4j

expected_dir, expected_version = sys.argv[1:]
if (
    py4j.__version__ != expected_version
    or not pathlib.Path(py4j.__file__).resolve().is_relative_to(expected_dir)
):
    sys.exit(
        f"[python-env] FATAL: expected baked Py4J {expected_version} in {expected_dir}; "
        f"loaded {py4j.__version__} from {py4j.__file__}. Rebuild the devcontainer image."
    )
PY
