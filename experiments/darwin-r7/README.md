# Round 7: six-view CDF walkthrough

Use the **primary** selector for a local, relative comparison against deployed v2. It reproduces the captured walkthrough: concurrent CREATE, INSERT and serial REFRESH, then DROP and re-CREATE over a twin source, the same INSERT and six concurrent REFRESHes. Only that last batch qualifies for the shortlist. The report binds the six requests to their views, physical source, source versions, watermarks and incremental outcomes. Parity is bidirectional `EXCEPT ALL`. The separate **companion** selector exercises a matched intercept path; it cannot qualify a CDF speedup. Neither selector runs in ordinary PR tests.

The primary clock starts before six requests are submitted and ends after the last request completes the walkthrough's command, INSIGHTS, queue drain, completed-span readback, query-log finalization, receipt write and release. SQL return, per-request phases and drain-return times are reported separately. An `awaitDrain` return is not a storage commit timestamp. The companion clock ends at the last REFRESH acknowledgement; never pool clocks or modes. `file://` data and modeled catalog/state-sync I/O are not OneLake measurements.

## Run a series

Use a clean detached checkout of the **independently frozen contract commit**, not a candidate branch. In the attached pinned devcontainer, use the repository's existing locked Nx installation. On its leased WSL host run `docker inspect --type container --format '{{.Id}} {{.Image}}' "$CONTAINER_ID"` and set `OPENIVM_R7_IMAGE_ID` to the returned full `sha256:...` image identity. At plan and arm start, the runner inspects its actual container. For a container without a Docker socket, the contract must instead pin the SHA-256 of `hostname + " " + image` from the independent WSL receipt; a recreated container needs new admission. A failed inspect never selects this mode automatically. Do not add a socket or privilege for measurement. Allocate the whole box to one timed series; do not start a build, second series or crash chain there.

```bash
export NX_DAEMON=false PYTHONDONTWRITEBYTECODE=1
python3 -m unittest discover -s experiments/darwin-r7
python3 experiments/darwin-r7/paired.py plan \
  --control 50fa4ceb2bfbe2419eb682503cfe20be2083f388 \
  --candidate "$PRODUCT_ONLY_COMMIT" --profile default --workload primary \
  --target spark-4.1 --series-id "$SERIES_ID" --out "$FRESH_ABSOLUTE_DIRECTORY"
```

`plan` starts **no arms**. Give D8 the series ID, plan SHA-256, full control/candidate, hypothesis family (lane and hypothesis label), profile, workload, target, lease/box, original Privy plan job ID, and the WSL job that read back the running container ID, hostname and image. Use the registration ID returned for that exact plan. Submit `run` as **one Privy job per registration** and give D8 its job ID at start, before retention can discard its stdout. An interrupted submission is not grounds to submit a second run.

```bash
python3 experiments/darwin-r7/paired.py run "$FRESH_ABSOLUTE_DIRECTORY" \
  --registered-plan-sha256 "$REGISTERED_PLAN_SHA256" --registration-id "$D8_REGISTRATION"
python3 experiments/darwin-r7/paired.py score "$FRESH_ABSOLUTE_DIRECTORY" \
  --anchors "$ORIGINAL_PRIVY_RUN_STDOUT"
```

The candidate must descend from the profile's control and modify only approved existing product sources. Keep supplementary tests on a different ref. The driver installs the identical trusted harness on both detached arms, checks native hashes, freezes the complete AB/BA/AB plan before running a command, and saves raw Nx output, reports, events and SHA-256 receipts outside the repository. Each invocation gets a fresh fork and warehouse; worktrees are reused only within the series. Do not reuse an output directory. Every `plan.json`, `registration.json` and `arm.json` digest is emitted to original job stdout; a locally regenerated transcript is not an independent receipt. The auditor reconciles it with the original job, its pre-run registration, and **every registered series** before admitting a shortlist. A repeat uses a new ID, never an omitted pair. No cross-host pooling.

For an **unscored** one-arm diagnosis, set `OPENIVM_STATE_GUARD_SUITE=r7-primary` (or `r7-companion`), `OPENIVM_STATE_GUARD_HEAD_SHA` to the product SHA, `OPENIVM_STATE_GUARD_RUN_LABEL` to a unique label and `OPENIVM_STATE_GUARD_REPORT_DIR` to a fresh absolute directory. Run `npx --no-install nx run spark-ext:state-guard-perf --configuration=spark-4.1 --skip-nx-cache`. Either Spark target is supported. `OPENIVM_STATE_GUARD_LATENCY_SCALE=1` is required for a scored run. Treat optional driver/task detail as an observer-cost comparison until the corresponding control profile is frozen.

`python3 experiments/darwin-r7/observer.py --out "$FRESH_ABSOLUTE_DIRECTORY"` runs three alternating legacy/deep pairs on the same provisional telemetry binary. The legacy arm keeps the walkthrough's queue enabled; the deep arm additionally enables both optional layers. Use a fresh checkout of the contract with installed locked Nx dependencies and the same image identity environment variable as above. Shared physical stages are joined by clock domain and stage ID, then counted once per attempt; callbacks after the last owner job ends are not covered. The observer series cannot shortlist a product or be subtracted from product timings.

## Interpret the result

Only a primary series in `shortlist_profiles` can shortlist. It requires three or more complete, same-host pairs, every pair improving and median unrounded durable reduction at least 3%. Deep instrumentation and the intercept companion are diagnostic even if all pairs improve.

**Every registered series counts.** D8 groups registrations by hypothesis family (lane and hypothesis label), profile, workload and target. A family needs at least one valid series, **every** valid series must shortlist, and every invalid or cancelled series needs a D8-accepted classification independent of performance. Timeouts and performance-dependent failures count as non-shortlisting; an unclassified failure cannot be discarded. A new series ID, candidate revision or box does not erase the earlier evidence. Do not pool pairs across hosts.

The report is **not a release approval**. The unchanged historical evaluator in `experiments/darwin` retains its 10% end-to-end / 80% targeted-wait rule and crash gates. Keep its actual decision, including `accepted=false`. The known `cdf/mv_version_and_cdf_watermark_gap` is a disclosed gap, not a passing recovery point. Original witness/Part B, regression and assembled-JAR gates, exact-tip CI and a fresh Fabric control are separate requirements.

Only the integrator revises the measurement contract. After reviewable edits, stage every new harness file, run `python3 experiments/darwin-r7/paired.py lock`, run the discriminator tests and obtain independent review of the new source and lock. A changed harness, profile, workload or policy invalidates reuse of earlier comparisons.
