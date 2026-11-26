#!/usr/bin/env python3
"""R7 paired-series driver; run only inside the pinned devcontainer.

  paired.py lock                      rewrite contract.lock.json from this tree's harness files
  paired.py plan --control SHA --candidate SHA --workload primary --profile default --out DIR
  paired.py run DIR --registered-plan-sha256 HASH --registration-id D8_ID
  paired.py score DIR --anchors ORIGINAL_PRIVY_STDOUT

`run` must start from a clean worktree at the contract commit. Each arm runs in a detached worktree of its own
commit, with every harness path overlaid byte-for-byte from the contract commit, so arms differ only in product code.
"""

import argparse
import contextlib
import hashlib
import json
import os
import platform
import re
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import scorecard  # noqa: E402

CONTRACT = HERE / "contract.json"
LOCK = HERE / "contract.lock.json"
NATIVE_DIR = Path("/opt/openivm")
NATIVE_ENV = {"OPENIVM_NATIVE_DIR": "/opt/openivm", "OPENIVM_CLI_PATH": "/opt/openivm/duckdb",
              "OPENIVM_EXTENSION_PATH": "/opt/openivm/openivm.duckdb_extension"}


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


def need(ok, reason):
    if not ok:
        raise SystemExit(f"refused: {reason}")


def write(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False, sort_keys=True) + "\n", encoding="utf-8")


def sha256(path):
    with open(path, "rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def host_identity(contract):
    node = platform.node()
    image = os.environ.get("OPENIVM_R7_IMAGE_ID", "")
    need(re.fullmatch(r"[a-f0-9]{12,64}", node), "cannot identify the running container by its hostname")
    bindings = contract["external_image_bindings"]
    if image in bindings:
        fingerprint = hashlib.sha256(f"{node} {image}".encode("ascii")).hexdigest()
        need(fingerprint in bindings[image], "container does not match the independently witnessed host-side readback")
    else:
        receipt = subprocess.check_output(
            ["docker", "inspect", "--type", "container", "--format", "{{.Id}} {{.Image}}", node], text=True).split()
        need(len(receipt) == 2 and re.fullmatch(r"[a-f0-9]{64}", receipt[0]) and receipt[0].startswith(node)
             and re.fullmatch(r"sha256:[a-f0-9]{64}", receipt[1]), "invalid running-container identity readback")
        need(receipt[1] == image, "running wrapper differs from the leased WSL readback")
    return {"node": node, "cpus": os.cpu_count(), "image": image}


def anchor(series, relative, directory):
    digest = sha256(directory / relative)
    print(f"@R7_RECEIPT {series} {relative} {digest}", flush=True)
    return digest


def harness_files(root, paths):
    files = [f for f in git(root, "ls-files", "-z", "--", *paths).split("\0") if f]
    return sorted(f for f in files if f != str(LOCK.relative_to(HERE.parent.parent)))


def harness_digest(root, paths):
    listing = {f: sha256(Path(root) / f) for f in harness_files(root, paths)}
    blob = "".join(f"{f} {h}\n" for f, h in listing.items()).encode()
    return hashlib.sha256(blob).hexdigest(), listing


def repo_root():
    return Path(git(HERE, "rev-parse", "--show-toplevel"))


def cmd_lock(_):
    contract = json.loads(CONTRACT.read_text())
    digest, listing = harness_digest(repo_root(), contract["harness_paths"])
    write(LOCK, {"schema": "openivm.r7-contract-lock/v1", "harness_digest": digest, "files": listing})
    print(digest)


def native_identity(contract):
    cli, ext = NATIVE_DIR / "duckdb", NATIVE_DIR / "openivm.duckdb_extension"
    loaded = subprocess.check_output(
        [str(cli), "-init", "/dev/null", "-unsigned", "-noheader", "-csv", "-c",
         f"LOAD '{ext}'; SELECT extension_version FROM duckdb_extensions() WHERE extension_name='openivm' AND loaded;"],
        text=True).strip()
    expected = contract["identities"]["native"]
    need(re.fullmatch(r"[a-f0-9]{7,40}", loaded) and expected["source"].startswith(loaded),
         f"loaded native {loaded!r} is not {expected['source']}")
    return {"source": expected["source"], "cli_sha256": sha256(cli), "extension_sha256": sha256(ext)}


def prepare(root, contract_sha, sha, dest, paths):
    need(not dest.exists(), f"refusing an existing arm source {dest}")
    git(root, "worktree", "add", "--detach", str(dest), sha)
    need(git(dest, "rev-parse", "HEAD") == sha, f"{dest} is not at {sha}")
    base = git(root, "merge-base", contract_sha, sha)
    touched = git(root, "diff", "--name-only", base, sha, "--", *paths)
    need(not touched, f"{sha} changes trusted harness paths: {touched.splitlines()[:10]}")
    git(dest, "checkout", contract_sha, "--", *paths)
    need(not git(dest, "ls-files", "--others", "--exclude-standard", "--", *paths), f"untracked harness files in {dest}")
    need(subprocess.run(["git", "-C", str(dest), "diff", "--quiet", contract_sha, "--", *paths]).returncode == 0,
         f"harness overlay in {dest} differs from the contract")
    modules = dest / "node_modules"
    if not modules.exists():
        modules.symlink_to(root / "node_modules")


def product_diff(root, contract, control, candidate):
    need(subprocess.run(["git", "-C", str(root), "merge-base", "--is-ancestor", control, candidate]).returncode == 0,
         "candidate must descend from the profile's control")
    names = set(contract["permitted_product_names"])
    paths = git(root, "ls-tree", "-r", "--name-only", control, "--", "spark-ext").splitlines()
    allowed = {p for p in paths if re.fullmatch(r"spark-ext/ivm-(?:common|compiler|extension|executor)/"
                                                 r"src/main/(?:scala|java)/org/openivm/spark/.+", p)
               and Path(p).name in names}
    diff = git(root, "diff", "--raw", "--no-renames", control, candidate).splitlines()
    need(diff, "candidate has no net product change")
    for row in diff:
        meta, path = row.split("\t", 1)
        need(meta.startswith(":100644 100644 ") and meta.endswith(" M") and path in allowed,
             f"candidate changes an unapproved path or file type: {path}")
    return [row.split("\t", 1)[1] for row in diff]


def run_arm(contract, plan, pair, arm, wt, out, timeout):
    workload = contract["workloads"][plan["workload"]]
    profile = contract["profiles"][plan["profile"]]
    label = f"{plan['series_id']}-p{pair:02d}-{arm}"
    directory = out / f"p{pair:02d}-{arm}"
    directory.mkdir()
    digest, _ = harness_digest(wt, contract["harness_paths"])
    sha = git(wt, "rev-parse", "HEAD")
    conf = ",".join(f"{k}={v}" for k, v in sorted(profile["telemetry_conf"].items()))
    env = dict(os.environ, **NATIVE_ENV, NX_DAEMON="false", OPENIVM_TEST_FORKS="1",
               OPENIVM_STATE_GUARD_SUITE=workload["selector"], OPENIVM_STATE_GUARD_REPORT_DIR=str(directory),
               OPENIVM_STATE_GUARD_HEAD_SHA=sha, OPENIVM_STATE_GUARD_RUN_LABEL=label,
               OPENIVM_STATE_GUARD_LATENCY_SCALE="1",
               OPENIVM_R7_INSIGHTS_POLL_MS=str(contract["expected"]["insights_poll_ms"]))
    env.pop("OPENIVM_R7_TELEMETRY_CONF", None)
    if conf:
        env["OPENIVM_R7_TELEMETRY_CONF"] = conf
    command = ["npx", "--no-install", "nx", "run", "spark-ext:state-guard-perf",
               f"--configuration={plan['target']}", "--skip-nx-cache"]
    record = {"pair": pair, "arm": arm, "sha": sha, "label": label, "harness_digest": digest,
              "host": host_identity(contract), "native": native_identity(contract), "command": command, "started_unix_s": time.time()}
    need(record["host"] == plan["host"] and record["native"] == plan["native"] and digest == plan["harness_digest"],
         "host, native or harness changed before arm")
    with (directory / "nx.log").open("w") as log:
        proc = subprocess.Popen(command, cwd=wt, env=env, stdout=log, stderr=subprocess.STDOUT,
                                start_new_session=True)
        try:
            code = proc.wait(timeout=timeout)
            status = "completed" if code == 0 else "failed"
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            code, status = proc.wait(), "cancelled-timeout"
    record.update(exit_code=code, status=status, ended_unix_s=time.time())
    report = directory / workload["report"]
    try:
        record["report"] = json.loads(report.read_text()) if report.is_file() else None
    except (UnicodeError, json.JSONDecodeError) as exc:
        record.update(report=None, status="invalid-report", error=str(exc))
    if status == "completed" and not isinstance(record["report"], dict):
        record["status"] = "invalid-report"
    log = re.sub(r"\x1b\[[0-9;]*m", "", (directory / "nx.log").read_text(errors="replace"))
    if status == "completed" and f"spark-ext:state-guard-perf:{plan['target']}" not in log:
        record["status"] = "failed-wrong-target"
    record["evidence_sha256"] = scorecard.evidence_digests(directory)
    write(directory / "arm.json", record)
    anchor(plan["series_id"], str(directory.relative_to(out) / "arm.json"), out)
    return record


def clean_contract():
    need(sys.platform == "linux" and Path("/.dockerenv").exists(), "run inside the pinned devcontainer")
    root = repo_root()
    contract = scorecard.load_contract(CONTRACT)
    need(not git(root, "status", "--porcelain"), "the contract worktree must be clean")
    contract_sha = git(root, "rev-parse", "HEAD")
    digest, _ = harness_digest(root, contract["harness_paths"])
    need(digest == contract["_harness_digest"], "contract.lock.json does not match the harness files")
    return root, contract, contract_sha


def cmd_plan(args):
    root, contract, contract_sha = clean_contract()
    profile = contract["profiles"][args.profile]
    need(profile["control_sha"] is not None, f"profile {args.profile} has no frozen control yet")
    control = git(root, "rev-parse", f"{args.control}^{{commit}}")
    candidate = git(root, "rev-parse", f"{args.candidate}^{{commit}}")
    need(control == profile["control_sha"], f"control must be {profile['control_sha']} for {args.profile}")
    changes = product_diff(root, contract, control, candidate)
    host = host_identity(contract)
    need(host["image"] in contract["qualified_images"], "actual wrapper image must be admitted by D1/D8 before planning")
    for sha in (control, candidate):
        for ancestor in profile.get("requires_ancestors", []):
            need(subprocess.run(["git", "-C", str(root), "merge-base", "--is-ancestor", ancestor, sha]).returncode == 0,
                 f"{sha} lacks {ancestor} required by profile {args.profile}")
    out = args.out.resolve()
    need(not out.is_relative_to(root), "evidence must be outside the source worktree")
    series = args.series_id or f"r7s-{uuid.uuid4().hex}"
    need(re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._-]{0,80}", series), "invalid series ID")
    out.mkdir(parents=True, exist_ok=False)
    plan = {"schema": "openivm.r7-series/v1", "series_id": series,
                "comparison_class": "code-ab", "workload": args.workload, "profile": args.profile,
                "target": args.target, "pairs": args.pairs, "order": scorecard.frozen_order(args.pairs),
                "control": {"sha": control}, "candidate": {"sha": candidate}, "contract_sha": contract_sha,
                "contract_digest": contract["_digest"], "harness_digest": contract["_harness_digest"],
                "native": native_identity(contract), "native_decision": None,
                "product_paths": changes, "host": host, "arm_timeout_s": args.arm_timeout,
                "exclusions": contract["exclusions"], "frozen_unix_s": time.time()}
    need(plan["native"] == contract["identities"]["native"], "native binaries differ from the contract")
    write(out / "plan.json", plan)
    anchor(series, "plan.json", out)
    print("No arms started. Register this ID, plan digest, host proof and original Privy job with D8.", flush=True)


def cmd_run(args):
    import fcntl
    root, contract, contract_sha = clean_contract()
    out = args.series.resolve()
    need(sha256(out / "plan.json") == args.registered_plan_sha256, "registered plan digest mismatch")
    plan = json.loads((out / "plan.json").read_text())
    problems = []
    scorecard.check_plan(contract, plan, problems)
    need(not problems, "; ".join(problems))
    need(plan["contract_sha"] == contract_sha and plan["host"] == host_identity(contract), "contract or host changed")
    control, candidate = (plan[a]["sha"] for a in scorecard.ARMS)
    need(product_diff(root, contract, control, candidate) == plan["product_paths"], "product diff changed")
    need(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", args.registration_id), "invalid D8 registration ID")
    with open("/tmp/openivm-r7-series.lock", "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        need({p.name for p in out.iterdir()} == {"plan.json"}, "series already started; register a new series for a retest")
        with (out / "registration.json").open("x") as f:
            json.dump({"id": args.registration_id, "series": plan["series_id"], "plan_sha256": args.registered_plan_sha256}, f)
        anchor(plan["series_id"], "plan.json", out)
        anchor(plan["series_id"], "registration.json", out)
        src = Path(f"{out}.src")
        paths = contract["harness_paths"]
        trees = {"control": src / "control", "candidate": src / "candidate"}
        for arm, sha in (("control", control), ("candidate", candidate)):
            prepare(root, contract_sha, sha, trees[arm], paths)
        failed = None
        for pair, arm in plan["order"]:
            if failed:
                write(out / f"p{pair:02d}-{arm}.not-run.json", {"pair": pair, "arm": arm, "reason": failed})
                continue
            print(f"p{pair:02d}-{arm}: {plan[arm]['sha']}", flush=True)
            record = run_arm(contract, plan, pair, arm, trees[arm], out, plan["arm_timeout_s"])
            if record["status"] != "completed":
                failed = f"p{pair:02d}-{arm} {record['status']} (exit {record['exit_code']})"
        if failed:
            raise SystemExit(failed)
        print("Arms finished. Score with the original Privy receipt stream; reconcile all registrations with D8.", flush=True)


def cmd_score(args):
    rc = scorecard.main(["scorecard.py", str(CONTRACT), str(args.series), str(args.anchors)])
    if rc:
        raise SystemExit(rc)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("lock").set_defaults(fn=cmd_lock)
    run = sub.add_parser("plan")
    run.add_argument("--control", required=True)
    run.add_argument("--candidate", required=True)
    run.add_argument("--workload", choices=["primary", "companion"], default="primary")
    run.add_argument("--profile", default="default")
    run.add_argument("--target", choices=["spark-3.5", "spark-4.1"], default="spark-4.1")
    run.add_argument("--pairs", type=int, default=3)
    run.add_argument("--out", type=Path, required=True)
    run.add_argument("--series-id")
    run.add_argument("--arm-timeout", type=int, default=3600)
    run.set_defaults(fn=cmd_plan)
    execute = sub.add_parser("run")
    execute.add_argument("series", type=Path)
    execute.add_argument("--registered-plan-sha256", required=True)
    execute.add_argument("--registration-id", required=True)
    execute.set_defaults(fn=cmd_run)
    score = sub.add_parser("score")
    score.add_argument("series", type=Path)
    score.add_argument("--anchors", required=True, type=Path)
    score.set_defaults(fn=cmd_score)
    args = parser.parse_args()
    if args.cmd == "plan":
        need(args.pairs >= 3, "at least three pairs")
        need(args.arm_timeout > 0, "positive per-arm timeout required")
    with contextlib.suppress(BrokenPipeError):
        args.fn(args)


if __name__ == "__main__":
    main()
