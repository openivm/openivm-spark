package org.openivm.spark.perf

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{
  CreateFlag,
  DelegateToFileSystem,
  FSDataInputStream,
  FSDataOutputStream,
  FileStatus,
  FsServerDefaults,
  LocalFileSystem,
  Path,
  RawLocalFileSystem
}
import org.apache.hadoop.fs.local.LocalConfigKeys
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

import java.io.{IOException, InterruptedIOException}
import java.net.URI
import java.util
import java.util.concurrent.{ConcurrentHashMap, CopyOnWriteArrayList}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

/** Modeled per-operation latency for the two areas the state guard touches. These are relative, local numbers used to
  * make state-sync and catalog finalization cost visible; they are not measurements of OneLake.
  */
final case class StateGuardLatency(
    name: String,
    catalogReadMs: Long,
    catalogWriteMs: Long,
    stateReadMs: Long,
    stateWriteMs: Long,
    stateRenameMs: Long,
    scale: Double
) {
  private def scaled(ms: Long): Long = math.max(0L, math.round(ms * scale))

  def delayMs(area: String, op: String): Long =
    if (area == StateGuardFsInjection.Catalog) {
      if (op == "read" || op == "list") scaled(catalogReadMs) else scaled(catalogWriteMs)
    } else if (op == "read" || op == "list") scaled(stateReadMs)
    else if (op == "rename") scaled(stateRenameMs)
    else scaled(stateWriteMs)
}

object StateGuardLatency {
  val Off: StateGuardLatency = StateGuardLatency("off", 0L, 0L, 0L, 0L, 0L, 1.0)

  /** Object-store-like per-call cost: small reads ~25 ms, writes ~60 ms, renames ~80 ms. */
  def modeled(scale: Double): StateGuardLatency =
    StateGuardLatency("object-store-modeled-v1", 25L, 60L, 25L, 60L, 80L, scale)
}

/** Process-wide interception state shared by [[StateGuardLocalFileSystem]] instances. Only the outermost filesystem
  * call per thread is counted and delayed, so composed operations (create -> mkdirs) are charged once.
  */
object StateGuardFsInjection {
  val StateSync          = "state_sync"
  val Catalog            = "catalog"
  val Areas: Seq[String] = Seq(StateSync, Catalog)
  val Ops: Seq[String]   = Seq("read", "write", "rename", "delete", "list")

  private final case class Armed(area: String, op: String, pathContains: String, remaining: AtomicInteger)

  private val counters = new ConcurrentHashMap[String, AtomicLong]()
  private val armed    = new CopyOnWriteArrayList[Armed]()
  private val depth    = new ThreadLocal[Array[Int]] { override def initialValue(): Array[Int] = Array(0) }

  @volatile private var latency: StateGuardLatency = StateGuardLatency.Off

  def configure(profile: StateGuardLatency): Unit = latency = profile
  def currentLatency: StateGuardLatency           = latency
  def disable(): Unit                             = latency = StateGuardLatency.Off

  def reset(): Unit = {
    counters.clear()
    armed.clear()
  }

  /** Fail the next `times` matching operations with an [[IOException]]. */
  def armFailure(area: String, op: String, pathContains: String, times: Int): Unit =
    armed.add(Armed(area, op, pathContains, new AtomicInteger(times)))

  def snapshot(): Map[String, Long] =
    (for (area <- Areas; op <- Ops)
      yield s"$area.$op" -> Option(counters.get(s"$area.$op")).map(_.get()).getOrElse(0L)).toMap

  private def classify(path: Path): String = {
    val value = path.toUri.getPath
    if (value == null || value.endsWith(".crc")) null
    else if (value.contains("/state-sync-remote")) StateSync
    else if (value.contains("/_openivm_catalog")) Catalog
    else null
  }

  private[perf] def around[A](paths: Seq[Path], op: String)(body: => A): A = {
    val state = depth.get()
    if (state(0) > 0) body
    else {
      val area = paths.iterator.map(classify).find(_ != null).orNull
      if (area == null) body
      else {
        state(0) += 1
        try {
          counters.computeIfAbsent(s"$area.$op", _ => new AtomicLong()).incrementAndGet()
          val delay = latency.delayMs(area, op)
          if (delay > 0L)
            try Thread.sleep(delay)
            catch {
              case _: InterruptedException =>
                Thread.currentThread().interrupt()
                throw new InterruptedIOException(s"state-guard latency interrupted for ${paths.head}")
            }
          armed.forEach { rule =>
            if (
              rule.area == area && rule.op == op && paths.exists(_.toString.contains(rule.pathContains)) &&
              rule.remaining.getAndUpdate(n => if (n > 0) n - 1 else 0) > 0
            ) throw new IOException(s"state-guard injected $area.$op failure for ${paths.head}")
          }
          body
        } finally state(0) -= 1
      }
    }
  }
}

/** Raw local filesystem that applies [[StateGuardFsInjection]] to state-sync and catalog paths. */
class StateGuardRawLocalFileSystem extends RawLocalFileSystem {
  import StateGuardFsInjection.around

  override def open(f: Path, bufferSize: Int): FSDataInputStream = around(Seq(f), "read")(super.open(f, bufferSize))

  override def getFileStatus(f: Path): FileStatus = around(Seq(f), "read")(super.getFileStatus(f))

  override def listStatus(f: Path): Array[FileStatus] = around(Seq(f), "list")(super.listStatus(f))

  override def create(
      f: Path,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable
  ): FSDataOutputStream =
    around(Seq(f), "write")(super.create(f, overwrite, bufferSize, replication, blockSize, progress))

  override def create(
      f: Path,
      permission: FsPermission,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable
  ): FSDataOutputStream =
    around(Seq(f), "write")(super.create(f, permission, overwrite, bufferSize, replication, blockSize, progress))

  override def createNonRecursive(
      f: Path,
      permission: FsPermission,
      flags: util.EnumSet[CreateFlag],
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable
  ): FSDataOutputStream =
    around(Seq(f), "write")(
      super.createNonRecursive(f, permission, flags, bufferSize, replication, blockSize, progress)
    )

  override def createNonRecursive(
      f: Path,
      permission: FsPermission,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable
  ): FSDataOutputStream =
    around(Seq(f), "write")(
      super.createNonRecursive(f, permission, overwrite, bufferSize, replication, blockSize, progress)
    )

  override def mkdirs(f: Path): Boolean = around(Seq(f), "write")(super.mkdirs(f))

  override def mkdirs(f: Path, permission: FsPermission): Boolean = around(Seq(f), "write")(super.mkdirs(f, permission))

  override def rename(src: Path, dst: Path): Boolean = around(Seq(dst, src), "rename")(super.rename(src, dst))

  override def delete(f: Path, recursive: Boolean): Boolean = around(Seq(f), "delete")(super.delete(f, recursive))
}

/** Checksummed local filesystem (`FileSystem.getLocal` casts to [[LocalFileSystem]]) over the injecting raw one. */
class StateGuardLocalFileSystem extends LocalFileSystem(new StateGuardRawLocalFileSystem)

/** AbstractFileSystem binding so Delta's FileContext-based log commits pass through the injecting filesystem. */
class StateGuardLocalAbstractFs(uri: URI, conf: Configuration)
    extends DelegateToFileSystem(uri, new StateGuardRawLocalFileSystem, conf, "file", false) {
  override def getUriDefaultPort(): Int = -1

  override def getServerDefaults(): FsServerDefaults = LocalConfigKeys.getServerDefaults()

  override def getServerDefaults(f: Path): FsServerDefaults = LocalConfigKeys.getServerDefaults()

  override def isValidName(src: String): Boolean = true
}
