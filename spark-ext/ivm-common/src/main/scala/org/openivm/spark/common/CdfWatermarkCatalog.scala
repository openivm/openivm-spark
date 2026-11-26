package org.openivm.spark.common

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.openivm.spark.common.rocksdb.{
  OpenIvmRocksDB,
  OpenIvmRocksDBBatchOps,
  OpenIvmRocksDBRegistry,
  OpenIvmStateBarrier,
  RocksDBCodec
}
import org.slf4j.LoggerFactory

import java.nio.file.Files
import scala.util.control.NonFatal

/**
 * Per-MV RocksDB catalog of last-consumed Delta versions.
 *
 * New state lives beside the owning MV metadata at
 * `<warehouse>/_openivm/mvs/<view>/rocksdb`, so independent refreshes never
 * contend on a global watermark database. Reads lazily migrate legacy rows
 * from the former shared index DB.
 */
private[common] object RocksDbCdfWatermarkBackend extends CdfWatermarkBackend {

  private val log = LoggerFactory.getLogger(getClass)

  private val Cf: String       = IndexDbColumnFamilies.CdfWatermarks
  private val LegacyCf: String = IndexDbColumnFamilies.CdfWatermarks

  private def openMvDb(spark: SparkSession, viewName: String): OpenIvmRocksDB =
    OpenIvmRocksDBRegistry.getOrOpen(
      spark,
      OpenIvmStatePaths.perMvDbPath(spark, viewName),
      OpenIvmStatePaths.PerMvColumnFamilies
    )

  private def sourceKey(source: String): Array[Byte] = RocksDBCodec.utf8(source)

  private def legacyKey(viewName: String, source: String): Array[Byte] =
    RocksDBCodec.compositeKey(Seq(RocksDBCodec.utf8(viewName), RocksDBCodec.utf8(source)))

  private def openLegacyIndexDb(spark: SparkSession): Option[OpenIvmRocksDB] = {
    val path = OpenIvmStatePaths.indexDbPath(spark)
    if (OpenIvmStatePaths.isExistingDb(path)) {
      Some(OpenIvmRocksDBRegistry.getOrOpen(spark, path, IndexDbColumnFamilies.All))
    } else None
  }

  def ensureTables(spark: SparkSession): Unit = {
    Files.createDirectories(OpenIvmStatePaths.mvsRoot(spark))
    ()
  }

  def get(spark: SparkSession, viewName: String, source: String): Option[Long] = {
    val db  = openMvDb(spark, viewName)
    val key = sourceKey(source)
    db.get(Cf, key).map(RocksDBCodec.decodeLongBE).orElse {
      openLegacyIndexDb(spark)
        .flatMap(_.get(LegacyCf, legacyKey(viewName, source)))
        .map { encoded =>
          db.withBatch { batch =>
            OpenIvmRocksDBBatchOps.put(db, batch, Cf, key, encoded)
          }
          RocksDBCodec.decodeLongBE(encoded)
        }
    }
  }

  def getAll(spark: SparkSession, viewName: String, sources: Seq[String]): Map[String, Long] = {
    val distinct = sources.distinct
    if (distinct.size <= 1 || !spark.conf.get("spark.openivm.cdf.watermarks.multiGet.enabled", "false").toBoolean) {
      distinct.flatMap(source => get(spark, viewName, source).map(source -> _)).toMap
    } else {
      val values = openMvDb(spark, viewName).multiGet(Cf, distinct.map(sourceKey))
      distinct
        .zip(values)
        .flatMap { case (source, encoded) =>
          encoded.map(RocksDBCodec.decodeLongBE).orElse(get(spark, viewName, source)).map(source -> _)
        }
        .toMap
    }
  }

  def put(spark: SparkSession, viewName: String, source: String, version: Long): Unit =
    putAll(spark, viewName, Map(source -> version))

  def putAll(spark: SparkSession, viewName: String, versionsBySource: Map[String, Long]): Unit = {
    if (versionsBySource.isEmpty) return
    val db = openMvDb(spark, viewName)
    db.withBatch { batch =>
      versionsBySource.foreach { case (source, version) =>
        OpenIvmRocksDBBatchOps.put(db, batch, Cf, sourceKey(source), RocksDBCodec.encodeLongBE(version))
      }
    }
  }

  def removeForBaseTable(spark: SparkSession, baseTable: String): Unit = {
    // Guard: only proceed with per-MV cleanup when the reverse dependency index
    // DB exists. If it does not exist, no MV has been registered with baseTable
    // as a source (MvCatalog.upsert creates the DB on first registration), so
    // there is nothing to clean up. Without this guard, MvCatalog.viewsForSource
    // would fall back to scanning all per-MV DBs via MvCatalog.list — the exact
    // open-all fanout that this reverse-index path eliminates.
    val depDbPath = OpenIvmStatePaths.sourceDependencyDbPath(spark, baseTable)
    if (OpenIvmStatePaths.isExistingDb(depDbPath)) {
      // Use the authoritative reverse dependency index instead of enumerating
      // existingMvDbPaths and opening every per-MV RocksDB. Under concurrent
      // source-sharing DROP+CREATE the old open-all fanout raced with sibling
      // MvCatalog.remove close+delete cycles, opening DBs mid-deletion and
      // amplifying the registry orphan race. viewsForSource validates each
      // reverse-index entry against authoritative per-MV metadata, so stale
      // entries (a dropped MV whose index row was not yet pruned) are silently
      // skipped rather than opening a being-deleted or absent DB.
      val key = sourceKey(baseTable)
      MvCatalog.viewsForSource(spark, baseTable).foreach { meta =>
        val serializedName = meta.name.database.fold(meta.name.identifier)(db => s"$db.${meta.name.identifier}")
        val path           = OpenIvmStatePaths.perMvDbPath(spark, serializedName)
        // A concurrent DROP may delete this DB between viewsForSource's metadata
        // read and the open below. Guard with isExistingDb first; the NonFatal
        // catch is then narrowed to that benign concurrent-deletion window only.
        if (OpenIvmStatePaths.isExistingDb(path)) {
          try {
            val db = openMvDb(spark, serializedName)
            if (db.get(Cf, key).nonEmpty) {
              db.withBatch(batch => OpenIvmRocksDBBatchOps.delete(db, batch, Cf, key))
            }
          } catch {
            case NonFatal(e) =>
              // The catch is benign ONLY when the DB genuinely disappeared under
              // us (the concurrent-DROP race). Re-check the path: if the DB is
              // still present the failure is a real watermark write error /
              // corruption / lock regression and MUST surface — never swallow it.
              if (OpenIvmStatePaths.isExistingDb(path)) {
                log.error(
                  s"openivm-cdf-watermark removeForBaseTable: cleanup of view '$serializedName' failed " +
                    s"while its RocksDB is still present; surfacing (not a concurrent-deletion race)",
                  e
                )
                throw e
              } else {
                log.warn(
                  s"openivm-cdf-watermark removeForBaseTable: skipped view '$serializedName' " +
                    s"(RocksDB concurrently deleted): ${e.getMessage}"
                )
              }
          }
        }
      }
    }

    // Legacy shared-index cleanup. The shared index stores (viewName, source)
    // composite keys and must be scanned by source value; it predates per-MV
    // sharding and cannot use the reverse-dependency index. Exercised only on
    // pre-sharding installations where the legacy DB exists on disk; new installs
    // never write to it, so the scan is a no-op.
    openLegacyIndexDb(spark).foreach { db =>
      val baseBytes = RocksDBCodec.utf8(baseTable)
      val victims = db
        .prefixScan(LegacyCf, Array.emptyByteArray)
        .flatMap { case (candidate, _) =>
          val parts = RocksDBCodec.splitComposite(candidate, 2)
          if (parts.length == 2 && java.util.Arrays.equals(parts(1), baseBytes)) Iterator.single(candidate.clone())
          else Iterator.empty
        }
        .toList
      if (victims.nonEmpty) {
        db.withBatch { batch =>
          victims.foreach(candidate => OpenIvmRocksDBBatchOps.delete(db, batch, LegacyCf, candidate))
        }
      }
    }
  }

  def removeForView(spark: SparkSession, viewName: String): Unit = {
    val path = OpenIvmStatePaths.perMvDbPath(spark, viewName)
    if (OpenIvmStatePaths.isExistingDb(path)) {
      val db      = openMvDb(spark, viewName)
      val victims = db.prefixScan(Cf, Array.emptyByteArray).map { case (key, _) => key.clone() }.toList
      if (victims.nonEmpty) {
        db.withBatch { batch =>
          victims.foreach(key => OpenIvmRocksDBBatchOps.delete(db, batch, Cf, key))
        }
      }
    }

    openLegacyIndexDb(spark).foreach { db =>
      val prefix  = RocksDBCodec.compositeKey(Seq(RocksDBCodec.utf8(viewName), Array.emptyByteArray))
      val victims = db.prefixScan(LegacyCf, prefix).map { case (key, _) => key.clone() }.toList
      if (victims.nonEmpty) {
        db.withBatch { batch =>
          victims.foreach(key => OpenIvmRocksDBBatchOps.delete(db, batch, LegacyCf, key))
        }
      }
    }
  }
}

object CdfWatermarkCatalog {
  private def backend(spark: SparkSession): CdfWatermarkBackend = CdfWatermarkBackend.forSession(spark)

  /** The watermark rows of one committed Delta version. A Delta version is immutable, so the rows stay exact for the
    * (table id, version) they were read at; readers revalidate that key against a listing of their own Delta log that
    * began after the read.
    */
  private final class CommittedWatermarks(val tableId: String, val version: Long, rows: Array[(String, String, Long)]) {
    def versions(viewName: String, sources: Seq[String]): Map[String, Long] = {
      val wanted = sources.toSet
      rows.iterator.collect {
        case (view, source, version) if view == viewName && wanted.contains(source) => source -> version
      }.toMap
    }
  }

  /** Largest watermark table whose rows are shared between readers; a larger table is read per view, filtered by the
    * backend, exactly as without sharing.
    */
  private val MaxSharedRows = 64
  private val SharedRoots   = 32

  private val committedVersions = new BoundedLruMap[String, CommittedWatermarks](SharedRoots)
  private val committedLoads    = new BoundedLruMap[String, Object](SharedRoots)
  private val largeRoots        = new BoundedLruMap[String, java.lang.Boolean](1024)

  /** The shared Delta watermark table at its latest committed version. Every call observes a listing of its own Delta
    * log that began after the call, so a commit by any driver is visible to the next read; concurrent reads of one log
    * share that listing, and the row scan is shared between reads of the same physical log, table id and version.
    * `None` sends the caller down the backend path, which also creates a missing watermark table, and serves every
    * table found to hold more than `MaxSharedRows` rows.
    */
  private def committed(spark: SparkSession): Option[CommittedWatermarks] =
    if (!FeatureGate.deltaCatalogEnabled(spark)) None
    else
      try {
        val path                             = FeatureGate.catalogPath(spark).stripSuffix("/") + "/cdf_watermarks"
        val log                              = DeltaLog.forTable(spark, new org.apache.hadoop.fs.Path(path))
        val logPath                          = log.logPath.toString
        def latest(): Option[(String, Long)] = DeltaLogRefreshes.refreshedVersion(path, log)
        def cached(key: (String, Long)): Option[CommittedWatermarks] =
          committedVersions.get(logPath).filter(entry => entry.tableId == key._1 && entry.version == key._2)
        def load(current: (String, Long)): Option[CommittedWatermarks] = {
          val rows = spark.read
            .format("delta")
            .load(path)
            .select("view_name", "source", "version")
            .limit(MaxSharedRows + 1)
            .collect()
            .map(row => (row.getString(0), row.getString(1), row.getLong(2)))
          if (rows.length > MaxSharedRows) {
            largeRoots.put(logPath, java.lang.Boolean.TRUE)
            committedVersions.remove(logPath)
            None
          } else {
            val entry = new CommittedWatermarks(current._1, current._2, rows)
            if (latest().contains(current)) committedVersions.put(logPath, entry)
            Some(entry)
          }
        }
        if (largeRoots.get(logPath).isDefined) None
        else
          latest().flatMap { key =>
            cached(key).orElse {
              committedLoads.getOrCreate(logPath)(new Object).synchronized {
                if (largeRoots.get(logPath).isDefined) None
                else cached(key).orElse(latest().flatMap(current => cached(current).orElse(load(current))))
              }
            }
          }
      } catch {
        case NonFatal(_) => None
      }

  def ensureTables(spark: SparkSession): Unit = backend(spark).ensureTables(spark)

  def get(spark: SparkSession, viewName: String, source: String): Option[Long] =
    committed(spark).fold(backend(spark).get(spark, viewName, source))(_.versions(viewName, Seq(source)).get(source))

  def getAll(spark: SparkSession, viewName: String, sources: Seq[String]): Map[String, Long] =
    if (sources.isEmpty) Map.empty
    else committed(spark).fold(backend(spark).getAll(spark, viewName, sources))(_.versions(viewName, sources))

  def put(spark: SparkSession, viewName: String, source: String, version: Long): Unit =
    backend(spark).put(spark, viewName, source, version)

  def putAll(spark: SparkSession, viewName: String, versionsBySource: Map[String, Long]): Unit =
    backend(spark).putAll(spark, viewName, versionsBySource)

  def removeForBaseTable(spark: SparkSession, baseTable: String): Unit =
    OpenIvmStateBarrier.withMutation(spark) { backend(spark).removeForBaseTable(spark, baseTable) }

  def removeForView(spark: SparkSession, viewName: String): Unit =
    backend(spark).removeForView(spark, viewName)
}
