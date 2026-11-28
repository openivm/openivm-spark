#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
directories=("$@")
if [[ "${#directories[@]}" -eq 0 ]]; then
    directories=("$REPO_ROOT/spark-ext/docs" "$REPO_ROOT/experiments/darwin/docs")
fi

status=0
for directory in "${directories[@]}"; do
    if [[ ! -d "$directory" ]]; then
        printf '[doc-names] Missing documentation directory: %s\n' "$directory" >&2
        status=1
        continue
    fi
    while IFS= read -r -d '' path; do
        name="${path##*/}"
        if [[ ! "$name" =~ ^[A-Z0-9]+(_[A-Z0-9]+)*\.md$ ]]; then
            printf '[doc-names] Expected SCREAMING_CASE.md: %s\n' "$path" >&2
            status=1
        fi
    done < <(find "$directory" -type f -iname '*.md' -print0)
done
exit "$status"
