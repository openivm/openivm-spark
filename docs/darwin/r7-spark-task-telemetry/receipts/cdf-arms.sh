#!/usr/bin/env bash
# usage: arms.sh <classes-dir> <classpath-export> <java-home> <prefix>
set -uo pipefail
CLASSES=$1 EXPORT=$2 JH=$3 PREFIX=$4
OUT=/tmp/r7-s3-cdf/arms-$PREFIX; mkdir -p "$OUT"
PERMS=("off legacy deep" "legacy deep off" "deep off legacy" "off deep legacy" "deep legacy off" "legacy off deep")
i=0
for p in "${PERMS[@]}"; do
  i=$((i+1))
  for a in $p; do
    L="$PREFIX-p$i"
    s=$(date -u +%FT%TZ); l0=$(cut -d' ' -f1-3 /proc/loadavg)
    /tmp/r7-s3-cdf/run.sh "$CLASSES" "$EXPORT" "$JH" "$a" "$L" > "$OUT/$L-$a.log" 2>&1; rc=$?
    grep -E '^RESULT' "$OUT/$L-$a.log" | cut -c8- > "$OUT/$L-$a.json"
    echo "$s $(date -u +%FT%TZ) $L $a rc=$rc load_before=[$l0] load_after=[$(cut -d' ' -f1-3 /proc/loadavg)]" >> "$OUT/order.txt"
    rm -rf /tmp/r7-s3-cdf-"$L"-"$a"-*
  done
done
echo DONE >> "$OUT/order.txt"
