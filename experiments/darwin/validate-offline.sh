#!/usr/bin/env bash
# Offline validation of the evaluator (no Darwin, Nx, Spark or network).
# Invoke through bash (the file is not marked executable): bash experiments/darwin/validate-offline.sh
set -euo pipefail
cd "$(git -C "$(dirname "${BASH_SOURCE[0]}")" rev-parse --show-toplevel)"
exec python3 -m unittest discover -s experiments/darwin/tests -t . "$@"
