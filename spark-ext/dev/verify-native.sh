#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=pins.env
source "$SCRIPT_DIR/pins.env"

cli="${OPENIVM_CLI_PATH:-${OPENIVM_NATIVE_DIR:-/opt/openivm}/duckdb}"
extension="${OPENIVM_EXTENSION_PATH:-${OPENIVM_NATIVE_DIR:-/opt/openivm}/openivm.duckdb_extension}"
if [[ ! -x "$cli" || ! -f "$extension" ]]; then
    echo "[verify-native] FATAL: native CLI or extension is missing: $cli, $extension" >&2
    exit 1
fi
extension_sql="${extension//\'/\'\'}"
actual="$("$cli" -init /dev/null -unsigned -noheader -csv -c \
    "LOAD '$extension_sql'; SELECT extension_version FROM duckdb_extensions() WHERE extension_name='openivm';")"
if [[ ! "$actual" =~ ^[0-9a-f]{7,40}$ || "$OPENIVM_COMMIT" != "$actual"* ]]; then
    echo "[verify-native] FATAL: image has '$actual'; pins.env requires $OPENIVM_COMMIT. Rebuild the devcontainer image." >&2
    exit 1
fi
echo "[verify-native] Native compiler matches source pin: $actual"
