package org.openivm.spark.common

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class EmptyGroupDeleteSpec extends AnyFunSpec with Matchers {

  private val merge =
    "WITH refresh_cte AS (select grp, sum(openivm_multiplicity * cnt) as cnt from delta.`/vd` group by grp) " +
      "MERGE INTO `mv__ivm_data` v USING refresh_cte d ON v.grp IS NOT DISTINCT FROM d.grp " +
      "WHEN MATCHED THEN UPDATE SET cnt = COALESCE(v.cnt + d.cnt, v.cnt, d.cnt) " +
      "WHEN NOT MATCHED THEN INSERT (grp, cnt) VALUES (d.grp, d.cnt)"
  private val ctas   = "CREATE OR REPLACE TABLE delta.`/vd` USING DELTA AS SELECT grp, cnt FROM `mv__ivm_data`"
  private val delete = "delete from `mv__ivm_data` where COALESCE(openivm_count_1, 0) = 0 and COALESCE(cnt, 0) = 0"

  describe("programDeletesEmptyGroups") {
    it("accepts a trailing emptied-group delete over the count column") {
      EmptyGroupDelete.programDeletesEmptyGroups(Seq(ctas, merge, delete), "mv__ivm_data", "cnt") shouldBe true
      EmptyGroupDelete.programDeletesEmptyGroups(Seq(ctas, merge, delete), "MV__IVM_DATA", "`cnt`") shouldBe true
    }

    it("accepts a qualified target and a single conjunct") {
      val qualified = "DELETE FROM `default`.`mv__ivm_data` WHERE COALESCE(`openivm_count_star`, 0) = 0;"
      EmptyGroupDelete.programDeletesEmptyGroups(Seq(merge, qualified), "mv__ivm_data", "openivm_count_star") shouldBe
        true
    }

    it("rejects a delete that does not cover the count column") {
      EmptyGroupDelete.programDeletesEmptyGroups(Seq(merge, delete), "mv__ivm_data", "openivm_count_star") shouldBe
        false
    }

    it("rejects a program whose last target write is not the delete") {
      val laterInsert = "INSERT INTO `mv__ivm_data` SELECT * FROM openivm_new_mv"
      EmptyGroupDelete.programDeletesEmptyGroups(Seq(merge, delete, laterInsert), "mv__ivm_data", "cnt") shouldBe false
      EmptyGroupDelete.programDeletesEmptyGroups(Seq(delete, merge), "mv__ivm_data", "cnt") shouldBe false
    }

    it("rejects deletes with other predicates or another target") {
      val otherPredicate = "delete from `mv__ivm_data` where COALESCE(cnt, 0) = 0 or grp IS NULL"
      val notZero        = "delete from `mv__ivm_data` where COALESCE(cnt, 0) = 1"
      val otherTarget    = "delete from `other__ivm_data` where COALESCE(cnt, 0) = 0"
      Seq(otherPredicate, notZero).foreach { sql =>
        EmptyGroupDelete.programDeletesEmptyGroups(Seq(merge, sql), "mv__ivm_data", "cnt") shouldBe false
      }
      EmptyGroupDelete.programDeletesEmptyGroups(Seq(merge, otherTarget), "mv__ivm_data", "cnt") shouldBe false
    }

    it("rejects a program without a delete") {
      EmptyGroupDelete.programDeletesEmptyGroups(Seq(ctas, merge), "mv__ivm_data", "cnt") shouldBe false
      EmptyGroupDelete.programDeletesEmptyGroups(Seq.empty, "mv__ivm_data", "cnt") shouldBe false
    }
  }
}
