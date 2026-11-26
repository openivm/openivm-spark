#!/usr/bin/env python3
"""Opt-in CDF worker/order diagnostics, never the canonical R7 score or a code A/B.

Inside the pinned devcontainer, run from a clean checkout:
    python3 contrib/r7-worker-order/run.py --out /tmp/unique-dir --image <verified-image-id>

Each cell is an independent JVM with the canonical seed, serial warm-up, CREATE order,
six-view bridge/drain path, and parity/watermark checks. Only the second refresh's
width/order changes. The default balances every width across all four block positions,
and runs both directions twice per width. The canonical suite itself is never edited.
"""

import argparse
import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time


PRODUCT = "50fa4ceb2bfbe2419eb682503cfe20be2083f388"
FIXTURE = "4554b63cf3779f46e55c120ecd994e9fc28d060d"
DIGEST = "a913995ccc2aaef93070a55887d97e6d7da77d31847c2505f888b145b3b85e26"
OLD, NEW = "R7WalkthroughCdfPerfSpec", "R7WorkerOrderCdfPerfSpec"
SUB = Path("spark-ext/ivm-it/src/test/scala/org/openivm/spark/perf")
CELLS = "1f,2r,6f,3r,2f,3f,1r,6r,3f,6r,2f,1r,6f,1f,3r,2r"
NATIVE = {
    "duckdb": "c41f957db5645d545a9caef430994cf262d24fad9297fef70c691395aadb43fb",
    "openivm.duckdb_extension": "55aa106dcc3904831a74edba91531e1cb579e62aceeee6c7afe63c88ceb1b05e",
}


def check(ok, why):
    if not ok:
        raise ValueError(why)


def sha(b):
    return hashlib.sha256(b).hexdigest()


def generated(source):
    check(sha(source) == DIGEST, "Canonical fixture checksum mismatch")
    s = source.decode().replace(OLD, NEW)
    for a, b in [
        ("openivm.r7-walkthrough-perf/v1", "openivm.r7-worker-order/v1"),
        ("primary:cdf-walkthrough-six-mv-insert", "diagnostic:cdf-walkthrough-worker-order"),
        ('"role"             -> "primary"', '"role"             -> "supplemental"'),
        ('"primary" -> Map(', '"diagnostic" -> Map('),
        ("r7-walkthrough-cdf.json", "r7-worker-order.json"),
        ("r7-walkthrough-events.jsonl", "r7-worker-order-events.jsonl"),
        ('"title"                   -> b.title,', '"batch_start_monotonic_ns" -> b.startNs,\n      "title" -> b.title,'),
        ('private val views     =', '''private val width = sys.env("OPENIVM_R7_WORKERS").toInt
  private val reverse = sys.env("OPENIVM_R7_REVERSE").toBoolean
  require(Set(1, 2, 3, 6).contains(width))
  private val views     ='''),
        ('try batch(title, views.map(v => name(v) -> s"REFRESH',
         'try batch(title, (if (title.startsWith("Parallel REFRESH") && reverse) views.reverse else views).map(v => name(v) -> s"REFRESH'),
        ('val (primary, fsOps, primaryQ, generationBefore, generationAfter) = refresh("Parallel REFRESH — six MVs", 6)',
         '''val listener = new R7WorkerOrderTasks
    val sc = spark.sparkContext
    sc.addSparkListener(listener)
    def clockAnchor: Seq[Long] = {
      val lo = System.nanoTime()
      val wall = System.currentTimeMillis()
      Seq(lo, wall, System.nanoTime())
    }
    val beforeAnchor = clockAnchor
    val (primary, fsOps, primaryQ, generationBefore, generationAfter) =
      try {
        val result = refresh("Parallel REFRESH — six MVs", width)
        val afterAnchor = clockAnchor
        listener.drain(sc)
        StateGuardReport.write("r7-worker-order-tasks.json", listener.trace ++ Map[String, Any](
          "anchor_before" -> beforeAnchor, "anchor_after" -> afterAnchor, "slots" -> sc.defaultParallelism,
          "batch_start_ns" -> result._1.startNs, "batch_end_ns" -> result._1.endNs,
          "bridge_return_ns" -> result._1.returnEndNs, "workers" -> width, "reverse" -> reverse))
        result
      } finally sc.removeSparkListener(listener)'''),
        ('"six_concurrent"         -> (concurrency(primary) == 6)',
         '"configured_concurrency" -> (concurrency(primary) == width)'),
        ('"workers"                      -> primary.workers,',
         '"workers" -> primary.workers,\n          "reverse" -> reverse,\n          "dispatch" -> primary.requests.map(_.view),'),
    ]:
        check(s.count(a) == 1, f"Unexpected canonical fixture structure at {a[:80]}")
        s = s.replace(a, b)
    return (s + "\n" + Path(__file__).with_name("Tasks.scala.in").read_text()).encode()


@contextlib.contextmanager
def install(path, data):
    with path.open("xb") as f:
        f.write(data)
    try:
        yield
    finally:
        check(path.read_bytes() == data, f"Generated suite changed; preserving {path}")
        path.unlink()


def main():
    a = argparse.ArgumentParser(description=__doc__)
    a.add_argument("--out", required=True, type=Path)
    a.add_argument("--image", required=True)
    a.add_argument("--cells", default=CELLS)
    args = a.parse_args()
    cells = args.cells.split(",")
    check(all(re.fullmatch(r"[1236][fr]", c) for c in cells), "Bad worker/order cell")
    check(Path("/.dockerenv").is_file(), "Requires the pinned devcontainer")
    root = Path(subprocess.check_output(["git", "rev-parse", "--show-toplevel"], text=True).strip())
    git = lambda *xs: subprocess.check_output(["git", "-C", root, *xs], text=True).strip()
    src = root / SUB / (OLD + ".scala")
    rendered = generated(src.read_bytes())
    check(not git("status", "--porcelain"), "Use a clean worktree")
    subprocess.run(["git", "-C", root, "merge-base", "--is-ancestor", FIXTURE, "HEAD"], check=True)
    check(not git("diff", "--name-only", FIXTURE, "HEAD", "--", "spark-ext", ".devcontainer", "nx.json",
                  "package-lock.json", "package.json"), "The preflight expects identical build and product code")
    for f, h in NATIVE.items():
        with (Path("/opt/openivm") / f).open("rb") as stream:
            check(hashlib.file_digest(stream, "sha256").hexdigest() == h, f"Unexpected {f}")
    cmd = ["npx", "--no-install", "nx", "run", "spark-ext:test", "--configuration=spark-4.1", "--",
           f"ivmIt/testOnly org.openivm.spark.perf.{NEW} -- -n org.openivm.spark.tags.StateGuardPerf"]
    out = args.out.resolve()
    check(not out.is_relative_to(root), "Evidence requires an immutable directory outside the checkout")
    with open("/tmp/openivm-r7-concurrency.lock", "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        out.mkdir(parents=True, exist_ok=False)
        identity = {"product": PRODUCT, "fixture": FIXTURE, "fixture_sha256": DIGEST, "source_head": git("rev-parse", "HEAD"),
                    "supplement_sha256": sha(rendered), "runner_sha256": sha(Path(__file__).read_bytes()),
                    "native": NATIVE, "image": args.image, "cells": cells, "command": cmd,
                    "purpose": "Unscored scheduling diagnostics; not r7-primary, a code A/B or Fabric."}
        (out / "identity.json").write_text(json.dumps(identity, indent=2) + "\n")
        for i, cell in enumerate(cells, start=1):
            here = out / f"{i:02d}-{cell}"
            here.mkdir()
            label = f"{out.name}-{here.name}"
            env = dict(os.environ, OPENIVM_STATE_GUARD_REPORT_DIR=str(here), OPENIVM_STATE_GUARD_HEAD_SHA=PRODUCT,
                       OPENIVM_STATE_GUARD_RUN_LABEL=label, OPENIVM_STATE_GUARD_LATENCY_SCALE="1",
                       OPENIVM_R7_WORKERS=cell[0], OPENIVM_R7_REVERSE=str(cell[1] == "r").lower(),
                       OPENIVM_TEST_FORKS="1", NX_DAEMON="false", NX_SKIP_NX_CACHE="true",
                       OPENIVM_NATIVE_DIR="/opt/openivm", OPENIVM_CLI_PATH="/opt/openivm/duckdb",
                       OPENIVM_EXTENSION_PATH="/opt/openivm/openivm.duckdb_extension")
            for key in ("OPENIVM_R7_INSIGHTS_POLL_MS", "OPENIVM_R7_TELEMETRY_CONF", "OPENIVM_STATE_GUARD_SUITE"):
                env.pop(key, None)
            print(f"{time.strftime('%H:%M:%S')} {label}", flush=True)
            with install(src.with_name(NEW + ".scala"), rendered), (here / "nx.log").open("w") as log:
                subprocess.run(cmd, cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
            r = json.loads((here / "r7-worker-order.json").read_text())
            d = r["diagnostic"]
            names = [f"openivm_demo.concurrent_{n}" for n in range(1, 7)]
            check(r["schema"] == "openivm.r7-worker-order/v1" and r["role"] == "supplemental" and
                  r["label"] == label and r["head_sha"] == PRODUCT and r["spark_version"] == "4.1.0" and
                  r["latency_profile"]["scale"] == 1 and r["insights_poll_ms"] == 1000 and not r["telemetry_conf"],
                  "Wrong diagnostic receipt")
            check(r["gates"] and all(r["gates"].values()) and not r["failures"] and d["incremental_count"] == 6 and
                  d["full_refresh_count"] == 0 and d["workers"] == int(cell[0]) and d["reverse"] == (cell[1] == "r")
                  and d["dispatch"] == (names[::-1] if cell[1] == "r" else names), "Wrong gates, width, or dispatch")
            check((here / "r7-worker-order-tasks.json").stat().st_size > 0 and not git("status", "--porcelain"),
                  "Missing task trace or changed worktree")
            print(f"{label} durable={d['durable_ms']}ms return={d['return_ms']}ms", flush=True)


if __name__ == "__main__":
    main()
