package org.openivm.spark.parity

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}

import org.apache.spark.storage.StorageLevel
import org.openivm.spark.parity.base.{InterceptMode, IvmParitySpecBase}

class R7CleanupSiblingCacheBoundarySpec extends IvmParitySpecBase("r7-sibling-cache-boundary") with InterceptMode {
  it("observes same-name sibling cache immediately before and after each Spark API") {
    val sibling = spark.newSession()
    val out     = Paths.get(sys.env("R7_CLEANUP_OUT"))
    Seq(true, false).foreach { explicitFirst =>
      val name = if (explicitFirst) "r7cl_boundary_explicit" else "r7cl_boundary_drop_only"
      spark.sql(s"CREATE TEMP VIEW $name AS SELECT 191 AS k")
      sibling.sql(s"CREATE TEMP VIEW $name AS SELECT 347 AS k")
      spark.catalog.cacheTable(name, StorageLevel.DISK_ONLY)
      sibling.catalog.cacheTable(name, StorageLevel.DISK_ONLY)
      val target      = spark.table(name)
      val independent = sibling.table(name)
      def record(phase: String): Unit = {
        val line =
          s"api-sibling-cache\t${spark.version}\t$name\t$phase\t${target.storageLevel}\t${independent.storageLevel}\n"
        Files.write(out, line.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
        ()
      }
      try {
        target.head().getInt(0) shouldBe 191
        independent.head().getInt(0) shouldBe 347
        target.storageLevel shouldBe StorageLevel.DISK_ONLY
        independent.storageLevel shouldBe StorageLevel.DISK_ONLY
        record("before")
        if (explicitFirst) {
          spark.catalog.uncacheTable(name)
          record("after-explicit-uncache")
        }
        spark.sql(s"DROP VIEW IF EXISTS $name")
        record("after-sql-drop")
        spark.sessionState.catalog.getTempView(name).isEmpty shouldBe true
        sibling.table(name).head().getInt(0) shouldBe 347
      } finally {
        spark.sql(s"DROP VIEW IF EXISTS $name")
        sibling.sql(s"DROP VIEW IF EXISTS $name")
      }
    }
  }
}
