package org.openivm.spark.perf

import com.fasterxml.jackson.databind.ObjectMapper

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

/** A half-open time interval on the JVM monotonic clock, in nanoseconds. */
private[spark] final case class StateGuardInterval(startNs: Long, endNs: Long) {
  def lengthNs: Long = math.max(0L, endNs - startNs)
}

private[spark] object StateGuardReport {
  val ReportDirEnv   = "OPENIVM_STATE_GUARD_REPORT_DIR"
  val RunLabelEnv    = "OPENIVM_STATE_GUARD_RUN_LABEL"
  val LatencyScale   = "OPENIVM_STATE_GUARD_LATENCY_SCALE"
  val HeadShaEnv     = "OPENIVM_STATE_GUARD_HEAD_SHA"
  val EvidenceClass  = "local-relative-modeled-latency"
  private val mapper = new ObjectMapper()

  def reportDir: File =
    new File(sys.env.getOrElse(ReportDirEnv, "target/state-guard-perf")).getAbsoluteFile

  def latencyScale: Double =
    sys.env.get(LatencyScale).flatMap(v => scala.util.Try(v.trim.toDouble).toOption).filter(_ >= 0.0).getOrElse(1.0)

  val CatalogBackendEnv = "OPENIVM_STATE_GUARD_CATALOG_BACKEND"

  def catalogBackend: String =
    sys.env.get(CatalogBackendEnv).map(_.trim.toLowerCase).filter(Set("delta", "rocksdb")).getOrElse("delta")

  def sparkTarget: String = if (scala.util.Properties.versionNumberString.startsWith("2.12")) "spark-3.5" else "spark-4.1"

  def headSha: String =
    sys.env.get(HeadShaEnv).filter(_.nonEmpty).getOrElse {
      scala.util
        .Try {
          val process = new ProcessBuilder("git", "rev-parse", "HEAD").redirectErrorStream(true).start()
          val out     = scala.io.Source.fromInputStream(process.getInputStream, "UTF-8").mkString.trim
          if (process.waitFor() == 0) out else ""
        }
        .getOrElse("")
    }

  def ms(ns: Long): Double = math.round(math.max(0L, ns) / 1e3) / 1e3

  def union(intervals: Seq[StateGuardInterval]): Option[StateGuardInterval] =
    if (intervals.isEmpty) None else Some(StateGuardInterval(intervals.map(_.startNs).min, intervals.map(_.endNs).max))

  private def sweep(intervals: Seq[StateGuardInterval]): Seq[(Long, Int)] =
    intervals.filter(_.lengthNs > 0L).flatMap(i => Seq((i.startNs, 1), (i.endNs, -1))).sortBy { case (t, d) => (t, d) }

  def maxConcurrent(intervals: Seq[StateGuardInterval]): Int = {
    var active = 0
    var peak   = 0
    sweep(intervals).foreach { case (_, d) =>
      active += d
      peak = math.max(peak, active)
    }
    peak
  }

  def overlapNs(intervals: Seq[StateGuardInterval], atLeast: Int): Long = {
    var active = 0
    var last   = 0L
    var total  = 0L
    sweep(intervals).foreach { case (t, d) =>
      if (active >= atLeast) total += t - last
      active += d
      last = t
    }
    total
  }

  private def toJava(value: Any): AnyRef = value match {
    case null            => null
    case None            => null
    case Some(inner)     => toJava(inner)
    case m: scala.collection.Map[_, _] =>
      val out = new java.util.LinkedHashMap[String, AnyRef]()
      m.toSeq.sortBy(_._1.toString).foreach { case (k, v) => out.put(k.toString, toJava(v)) }
      out
    case s: scala.collection.Seq[_] =>
      val out = new java.util.ArrayList[AnyRef]()
      s.foreach(v => out.add(toJava(v)))
      out
    case d: Double =>
      require(!d.isNaN && !d.isInfinite, "non-finite number in report")
      java.lang.Double.valueOf(d)
    case l: Long    => java.lang.Long.valueOf(l)
    case i: Int     => java.lang.Integer.valueOf(i)
    case b: Boolean => java.lang.Boolean.valueOf(b)
    case s: String  => s
    case other      => throw new IllegalArgumentException(s"unsupported report value ${other.getClass.getName}")
  }

  def writeLines(name: String, lines: Seq[String]): File = {
    val dir = reportDir
    dir.mkdirs()
    val target = new File(dir, name)
    Files.write(target.toPath, lines.mkString("", "\n", "\n").getBytes(StandardCharsets.UTF_8))
    target
  }

  def write(name: String, report: Map[String, Any]): File = {
    val dir = reportDir
    dir.mkdirs()
    val target = new File(dir, name)
    val temp   = new File(dir, s"$name.tmp")
    Files.write(
      temp.toPath,
      mapper.writerWithDefaultPrettyPrinter().writeValueAsString(toJava(report)).getBytes(StandardCharsets.UTF_8)
    )
    Files.move(temp.toPath, target.toPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING)
    target
  }
}
