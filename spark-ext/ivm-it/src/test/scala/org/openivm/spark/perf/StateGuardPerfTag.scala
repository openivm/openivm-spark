package org.openivm.spark.perf

import org.scalatest.Tag

/** Opt-in state-guard performance and crash-recovery suites. Excluded from ordinary test runs. */
object StateGuardPerfTag extends Tag("org.openivm.spark.tags.StateGuardPerf")
