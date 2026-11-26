# Round 7: six-view CDF walkthrough

Use the **primary** selector for a local, relative comparison against deployed v2. It reproduces the captured walkthrough: concurrent CREATE, INSERT and serial REFRESH, then DROP and re-CREATE over a twin source, the same INSERT and six concurrent REFRESHes. Only that last batch qualifies for the shortlist. The report binds the six requests to their views, physical source, source versions, watermarks and incremental outcomes. Parity is bidirectional `EXCEPT ALL`. The separate **companion** selector exercises a matched intercept path; it cannot qualify a CDF speedup. Neither selector runs in ordinary PR tests.

The primary clock starts before six requests are submitted and ends after the last request completes the walkthrough's command, INSIGHTS, queue drain, completed-span readback, query-log finalization, receipt write and release. SQL return, per-request phases and drain-return times are reported separately. An `awaitDrain` return is not a storage commit timestamp. The companion clock ends at the last REFRESH acknowledgement; never pool clocks or modes. `file://` data and modeled catalog/state-sync I/O are not OneLake measurements.

## Run a series

Use a clean detached checkout of the **independently frozen contract commit**, not a candidate branch. In the attached pinned devcontainer, use the repository's existing locked Nx installation. Record `docker inspect --format '{{.Image}}'` for that container on its leased WSL host, then set `OPENIVM_R7_IMAGE_ID` to the returned full `sha256:...` identity. Allocate the whole box to one timed series. Do not start a build, second series or crash chain there.

```bash
export NX_DAEMON=false PYTHONDONTWRITEBYTECODE=1
python3 -m unittest discover -s experiments/darwin-r7
python3 experiments/darwin-r7/paired.py run \
  --control 50fa4ceb2bfbe2419eb682503cfe20be2083f388 \
  --candidate "$PRODUCT_ONLY_COMMIT" --profile default --workload primary \
  --target spark-4.1 --series-id "$SERIES_ID" --out "$FRESH_ABSOLUTE_DIRECTORY"
```

The candidate must descend from the profile's control and modify only approved existing product sources. Keep supplementary tests on a different ref. The driver installs the identical trusted harness on both detached arms, checks native hashes, freezes the complete AB/BA/AB plan before running a command, and saves raw Nx output, reports, events and SHA-256 receipts outside the repository. Each invocation gets a fresh fork and warehouse; worktrees are reused only within the series. Do not reuse an output directory. `paired.py score DIRECTORY` recomputes the verdict from its original evidence. Preserve failed, cancelled and superseded attempts. Retest under a new ID, never by dropping a pair.

For an **unscored** one-arm diagnosis, set `OPENIVM_STATE_GUARD_SUITE=r7-primary` (or `r7-companion`), `OPENIVM_STATE_GUARD_HEAD_SHA` to the product SHA, `OPENIVM_STATE_GUARD_RUN_LABEL` to a unique label and `OPENIVM_STATE_GUARD_REPORT_DIR` to a fresh absolute directory. Run `npx --no-install nx run spark-ext:state-guard-perf --configuration=spark-4.1 --skip-nx-cache`. Either Spark target is supported. `OPENIVM_STATE_GUARD_LATENCY_SCALE=1` is required for a scored run. Treat optional driver/task detail as an observer-cost comparison until the corresponding control profile is frozen.

`python3 experiments/darwin-r7/observer.py --out "$FRESH_ABSOLUTE_DIRECTORY"` runs three alternating legacy/deep pairs on the same provisional telemetry binary. The legacy arm keeps the walkthrough's queue enabled; the deep arm additionally enables both optional layers. Use a fresh checkout of the contract with installed locked Nx dependencies and the same image identity environment variable as above. Shared physical stages are joined by clock domain and stage ID, then counted once per attempt; callbacks after the last owner job ends are not covered. The observer series cannot shortlist a product or be subtracted from product timings.

## Interpret the result

The shortlist requires three or more complete, same-host paired runs, every pair improving and median unrounded durable reduction at least 3%. The report is **not a release approval**. The unchanged historical evaluator in `experiments/darwin` retains its 10% end-to-end / 80% targeted-wait rule and crash gates. Keep its actual decision, including `accepted=false`. The known `cdf/mv_version_and_cdf_watermark_gap` is a disclosed gap, not a passing recovery point. Original witness/Part B, regression and assembled-JAR gates, exact-tip CI and a fresh Fabric control are separate requirements.

Only the integrator revises the measurement contract. After reviewable edits, stage every new harness file, run `python3 experiments/darwin-r7/paired.py lock`, run the discriminator tests and obtain independent review of the new source and lock. A changed harness, profile, workload or policy invalidates reuse of earlier comparisons.
