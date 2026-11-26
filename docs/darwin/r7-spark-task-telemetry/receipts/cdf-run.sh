#!/usr/bin/env bash
# usage: run.sh <classes-dir> <classpath-export> <java-home> <arm> <label>
set -euo pipefail
CLASSES=$1 EXPORT=$2 JH=$3 ARM=$4 LABEL=$5
OPENS=$(grep -oE '^-J.*' /workspaces/openivm-spark/spark-ext/.sbtopts | sed 's/^-J//' | grep -E '^--add-(opens|exports)' | xargs)
exec "$JH/bin/java" -Xmx6g $OPENS -Dlog4j2.level=ERROR -cp "$CLASSES:$(tail -n1 "$EXPORT")" CdfObserverProbe "$ARM" "$LABEL"
