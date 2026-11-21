package org.openivm.spark.common.rocksdb

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import scala.jdk.CollectionConverters._

class OpenIvmCheckpointReuseSpec extends AnyFunSpec with Matchers {
  describe("checkpoint reuse") {
    it("reuses unchanged live stores and falls back on changes, corruption, and replacement") {
      val root = Files.createTempDirectory("openivm-checkpoint-reuse-test-")
      val live = root.resolve("live")
      var db   = new OpenIvmRocksDB(live.toString, OpenIvmRocksDBConf.default, Seq("meta"))
      def write(value: String): Unit =
        db.withBatch(batch => db.put(batch, "meta", RocksDBCodec.utf8("k"), RocksDBCodec.utf8(value)))
      def capture(name: String): OpenIvmCheckpointReuse.Capture =
        OpenIvmCheckpointReuse.checkpoint(db, root.resolve(name))
      def restore(name: String, expected: String): Unit = {
        val restored = new OpenIvmRocksDB(root.resolve(name).toString, OpenIvmRocksDBConf.default, Seq("meta"))
        try {
          restored.load()
          RocksDBCodec.fromUtf8(restored.get("meta", RocksDBCodec.utf8("k")).get) shouldBe expected
        } finally restored.close()
      }
      try {
        db.load()
        write("old")
        val first = capture("first")
        first.outcome shouldBe "fresh"
        first.retain.foreach(_())
        capture("unchanged").outcome shouldBe "reuse"
        restore("unchanged", "old")

        write("changed")
        val changed = capture("changed")
        changed.outcome shouldBe "fallback"
        changed.retain.foreach(_())
        restore("changed", "changed")

        val manifest = live.resolve("openivm-manifests").resolve("MANIFEST-2")
        Files.write(manifest, Files.readAllBytes(manifest) ++ Array(' '.toByte))
        val sidecar = capture("sidecar")
        sidecar.outcome shouldBe "fallback"
        sidecar.retain.foreach(_())
        Files.readAllBytes(root.resolve("sidecar/openivm-manifests/MANIFEST-2")) shouldBe Files.readAllBytes(manifest)

        OpenIvmCheckpointReuse.corruptForTesting(db)
        val corrupt = capture("corrupt")
        corrupt.outcome shouldBe "fallback"
        corrupt.retain.foreach(_())
        restore("corrupt", "changed")

        OpenIvmCheckpointReuse.clearForTesting()
        val lost = capture("lost")
        lost.outcome shouldBe "fresh"
        lost.retain.foreach(_())

        db.close()
        val paths = Files.walk(live)
        try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.delete(_))
        finally paths.close()
        db = new OpenIvmRocksDB(live.toString, OpenIvmRocksDBConf.default, Seq("meta"))
        db.load()
        write("replacement")
        capture("replacement").outcome shouldBe "fresh"
        restore("replacement", "replacement")
      } finally {
        db.close()
        OpenIvmCheckpointReuse.clearForTesting()
        val paths = Files.walk(root)
        try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.delete(_))
        finally paths.close()
      }
    }
  }
}
