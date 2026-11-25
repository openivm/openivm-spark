# Darwin round 6: integration ledger

The composition base is `36bcad4e1e43795ee364d9a5963921d8c3e3f3f9`, pinned to native `57b21876b866b2fa7176a3ad35246f4daf90287e`. Freeze is 23:10 America/New_York on November 25, 2026; the round ends at 00:40 on November 26. No change to the PR or Fabric is part of the builder's work.

| Input | Evidence | Disposition |
|---|---|---|
| Measured winner `742eac691301dae7ea285afcb881913ed917dd29` | Three paired Fabric runs against `1f31680e562cb0e9cd70220abd43b2098029fc01`, both at native `57b21876`. Median paired makespan reduction 11.57% (1.131x). | Historical control, not a result for the composition base. |
| Exact base `36bcad4e` | [GCI](https://github.com/openivm/openivm-spark/actions/runs/45666722017) passed. The published image tag `09a5d1572aca7d969f8b087bee443f3c254fa86943e597758aa8f1b2d9591c2e` resolved in CI to `sha256:41f957495e4a582630303a012c1ae86598e3e408832ada764889fc0fd64f8b8e`. No assembly artifact was uploaded. Ordinary pulls on both available builders were denied. | No exact-base JAR receipt. Do not substitute the measured winner, a locally derived image tag, or another native pin. |
| Side qualification `7e17e0a33b1313680fba02a275877ab9b9a77c0e` | Native `085e418a`, not the round-6 native. [Side GCI](https://github.com/openivm/openivm-spark/actions/runs/45702898135) passed; independent Spark 4.1 full-suite fresh-process checks exceeded their deadlines. | Excluded from composition. Isolated passing gates do not cancel out a failing full-suite gate. |

No lane candidate has been accepted yet. A candidate needs an exact source and JAR receipt, at least three paired local samples with median gain of at least 3%, unchanged incremental and parity results, both runtime gates, and fresh-process crash evidence (19 gated points and 12 concrete tests). The catalog-version/CDF-watermark gap stays separately identified, not a twentieth gated pass. Individual lane results are not evidence for a merged tree.
