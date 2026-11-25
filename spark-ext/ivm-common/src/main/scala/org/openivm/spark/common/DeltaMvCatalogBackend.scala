package org.openivm.spark.common

import io.delta.tables.DeltaTable
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.functions.{
  array,
  array_contains,
  coalesce,
  col,
  concat,
  exists,
  explode,
  greatest,
  lit,
  map_concat,
  map_filter,
  map_from_arrays,
  map_keys,
  not,
  substring,
  when
}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.openivm.spark.telemetry.metrics.OpenIvmMetrics

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/** Delta-backed authoritative MV catalog for multi-driver deployments. */
private[common] object DeltaMvCatalogBackend extends MvCatalogBackend with DeltaRetrySupport {

  override def withDeltaRetry[T](operation: => T): T =
    RetryPolicy.DeltaCatalogConflicts.executeWithRetryCallback((_, _) =>
      OpenIvmMetrics.recordCatalogRetry("mv_catalog")
    )(operation)

  private val Name                    = "name"
  private val QuerySql                = "query_sql"
  private val RefreshType             = "refresh_type"
  private val RefreshTypeName         = "refresh_type_name"
  private val LastVersion             = "last_version"
  private val SourceTables            = "source_tables"
  private val SourceSchemaFingerprint = "source_schema_fingerprint"
  private val Location                = "location"
  private val CreatedAt               = "created_at"
  private val Properties              = "properties"

  private val schema = StructType(
    Seq(
      StructField(Name, StringType, nullable = false),
      StructField(QuerySql, StringType, nullable = false),
      StructField(RefreshType, IntegerType, nullable = false),
      StructField(RefreshTypeName, StringType, nullable = false),
      StructField(LastVersion, LongType, nullable = false),
      StructField(SourceTables, ArrayType(StringType, containsNull = false), nullable = false),
      StructField(SourceSchemaFingerprint, StringType, nullable = false),
      StructField(Location, StringType, nullable = false),
      StructField(CreatedAt, TimestampType, nullable = false),
      StructField(Properties, MapType(StringType, StringType, valueContainsNull = false), nullable = false)
    )
  )

  /** Hidden `properties` keys holding the view's live CDF watermark per source as `v:<version>`. They are written in the
    * advance commit, stripped from [[MvMetadata]], and kept by every metadata rewrite, like rows of the separate
    * `cdf_watermarks` table they stand in for.
    */
  private[common] val LiveCdfWatermarkKeyPrefix = "_ivm_cdf_watermark:"

  private def isLiveCdfWatermark(key: Column): Column = key.startsWith(LiveCdfWatermarkKeyPrefix)

  private def visibleProperties(properties: Column): Column =
    map_filter(properties, (key, _) => not(isLiveCdfWatermark(key)))

  private def liveCdfWatermarkProperties(properties: Column): Column =
    map_filter(properties, (key, _) => isLiveCdfWatermark(key))

  private def path(spark: SparkSession): String = FeatureGate.catalogPath(spark).stripSuffix("/") + "/mv_metadata"

  private def serializedName(id: TableIdentifier): String =
    id.database.fold(id.identifier)(db => s"$db.${id.identifier}")

  private def singleRow(spark: SparkSession, meta: MvMetadata) = {
    val row = Row(
      serializedName(meta.name),
      meta.querySql,
      meta.refreshType,
      meta.refreshTypeName,
      meta.lastVersion,
      meta.sourceTables,
      meta.sourceSchemaFingerprint,
      meta.location,
      meta.createdAt,
      meta.properties
    )
    spark.createDataFrame(spark.sparkContext.parallelize(Seq(row), 1), schema)
  }

  private def decode(row: Row): MvMetadata =
    MvMetadata(
      name = CatalystSqlParser.parseTableIdentifier(row.getAs[String](Name)),
      querySql = row.getAs[String](QuerySql),
      refreshType = row.getAs[Int](RefreshType),
      refreshTypeName = row.getAs[String](RefreshTypeName),
      lastVersion = row.getAs[Long](LastVersion),
      sourceTables = row.getSeq[String](row.fieldIndex(SourceTables)).toSeq,
      sourceSchemaFingerprint = row.getAs[String](SourceSchemaFingerprint),
      location = row.getAs[String](Location),
      createdAt = row.getAs[java.sql.Timestamp](CreatedAt),
      properties = row
        .getJavaMap[String, String](row.fieldIndex(Properties))
        .asScala
        .toMap
        .filterNot { case (key, _) => key.startsWith(LiveCdfWatermarkKeyPrefix) }
    )

  override def ensureTables(spark: SparkSession): Unit = OpenIvmMetrics.time("catalog.mv_catalog.ensure_tables") {
    withDeltaRetry {
      DeltaPathTable.createIfAbsent(spark, path(spark), schema, Name)
    }
  }

  private def incomingColumns: Map[String, Column] =
    schema.fieldNames.map(field => field -> col(s"incoming.$field")).toMap +
      (Properties -> visibleProperties(col(s"incoming.$Properties")))

  override def upsert(spark: SparkSession, meta: MvMetadata): Unit = OpenIvmMetrics.time("catalog.mv_catalog.upsert") {
    withDeltaRetry {
      ensureTables(spark)
      val incoming     = singleRow(spark, meta)
      val expectedName = serializedName(meta.name)
      DeltaTable
        .forPath(spark, path(spark))
        .as("target")
        .merge(
          incoming.as("incoming"),
          col(s"target.$Name") === lit(expectedName) && col(s"target.$Name") === col(s"incoming.$Name")
        )
        .whenMatched()
        .update(
          incomingColumns +
            (Properties -> map_concat(
              visibleProperties(col(s"incoming.$Properties")),
              liveCdfWatermarkProperties(col(s"target.$Properties"))
            ))
        )
        .whenNotMatched()
        .insert(incomingColumns)
        .execute()
    }
  }

  override def lookup(spark: SparkSession, name: TableIdentifier): Option[MvMetadata] = {
    ensureTables(spark)
    spark.read
      .format("delta")
      .load(path(spark))
      .filter(col(Name) === lit(serializedName(name)))
      .take(1)
      .headOption
      .map(decode)
  }

  override def lookupReadOnly(spark: SparkSession, name: TableIdentifier): Option[MvMetadata] = {
    val catalogPath = new org.apache.hadoop.fs.Path(path(spark))
    val fs          = catalogPath.getFileSystem(spark.sessionState.newHadoopConf())
    if (!fs.exists(catalogPath)) None
    else {
      val rows = spark.read
        .format("delta")
        .load(path(spark))
        .filter(col(Name) === lit(serializedName(name)))
        .take(2)
      if (rows.length > 1)
        throw new IllegalStateException(s"Duplicate MV registration for ${serializedName(name)}")
      rows.headOption.map { row =>
        if (row.anyNull) throw new IllegalStateException(s"Incomplete MV registration for ${serializedName(name)}")
        decode(row)
      }
    }
  }

  override def list(spark: SparkSession): Seq[MvMetadata] = {
    ensureTables(spark)
    spark.read.format("delta").load(path(spark)).orderBy(col(Name)).collect().toSeq.map(decode)
  }

  override def viewsForSource(spark: SparkSession, table: String): Seq[MvMetadata] = {
    ensureTables(spark)
    spark.read
      .format("delta")
      .load(path(spark))
      .filter(array_contains(col(SourceTables), table))
      .orderBy(col(Name))
      .collect()
      .toSeq
      .map(decode)
  }

  override def advance(spark: SparkSession, name: TableIdentifier, newVersion: Long): Unit = OpenIvmMetrics.time(
    "catalog.mv_catalog.advance"
  ) {
    withDeltaRetry {
      ensureTables(spark)
      DeltaTable
        .forPath(spark, path(spark))
        .update(
          col(Name) === lit(serializedName(name)) && col(LastVersion) < lit(newVersion),
          Map(LastVersion -> lit(newVersion))
        )
    }
  }

  private[common] def advanceWithInterceptWitness(
      spark: SparkSession,
      name: TableIdentifier,
      newVersion: Long,
      token: String
  ): Unit = OpenIvmMetrics.time("catalog.mv_catalog.advance") {
    withDeltaRetry {
      ensureTables(spark)
      val key      = StagingCatalog.CompletedRefreshTokenKey
      val retained = map_filter(col(Properties), (existingKey, _) => existingKey =!= lit(key))
      val proof    = map_from_arrays(array(lit(key)), array(lit(token)))
      DeltaTable
        .forPath(spark, path(spark))
        .update(
          col(Name) === lit(serializedName(name)) && col(LastVersion) <= lit(newVersion),
          Map(LastVersion -> lit(newVersion), Properties -> map_concat(retained, proof))
        )
    }
  }

  /** `entries` advance with `last_version`; `live` are the view's live CDF watermarks, which move forward in the same
    * commit even when the version does not, as a forward-only watermark put would.
    */
  private final case class CdfAdvance(
      id: TableIdentifier,
      newVersion: Long,
      entries: Seq[(String, Long)],
      live: Seq[(String, Long)] = Nil
  ) {
    val name: String = serializedName(id)
  }

  private def withCdfWatermarks(properties: Column, entries: Seq[(String, Long)]): Column =
    if (entries.isEmpty) properties
    else {
      val keys = array(entries.map { case (key, _) => lit(key) }: _*)
      val values = array(entries.map { case (key, version) =>
        val existing = properties.getItem(key)
        val previous = when(existing.startsWith("v:"), substring(existing, 3, 20).cast("long"))
          .otherwise(lit(Long.MinValue))
        coalesce(
          concat(lit("v:"), greatest(previous, lit(version)).cast("string")),
          lit(ChangeWatermark.DeltaVersion(version).encode)
        )
      }: _*)
      val retained = map_filter(properties, (key, _) => not(array_contains(keys, key)))
      map_concat(retained, map_from_arrays(keys, values))
    }

  private def advancedProperties(request: CdfAdvance): Column = {
    val advanced =
      if (request.entries.isEmpty) col(Properties)
      else
        when(col(LastVersion) < lit(request.newVersion), withCdfWatermarks(col(Properties), request.entries))
          .otherwise(col(Properties))
    withCdfWatermarks(advanced, request.live)
  }

  private def advanceOne(spark: SparkSession, request: CdfAdvance): Unit =
    if (request.live.nonEmpty)
      withDeltaRetry {
        ensureTables(spark)
        DeltaTable
          .forPath(spark, path(spark))
          .update(
            col(Name) === lit(request.name),
            Map(
              LastVersion -> greatest(col(LastVersion), lit(request.newVersion)),
              Properties  -> advancedProperties(request)
            )
          )
      }
    else if (request.entries.isEmpty)
      advance(spark, request.id, request.newVersion)
    else
      withDeltaRetry {
        ensureTables(spark)
        DeltaTable
          .forPath(spark, path(spark))
          .update(
            col(Name) === lit(request.name) && col(LastVersion) < lit(request.newVersion),
            Map(
              LastVersion -> lit(request.newVersion),
              Properties  -> withCdfWatermarks(col(Properties), request.entries)
            )
          )
      }

  /** One Delta UPDATE that applies each request's single-view advance to its own row (rows are keyed by name).
    *
    * The condition names only `name` partitions, so Delta rewrites those rows in one pass instead of first scanning
    * them for matches. The per-row advance condition moves into the assignments: a row whose stored version is not
    * older keeps its version and versioned watermarks, while its live CDF watermarks still move forward.
    */
  private def advanceBatch(spark: SparkSession, requests: Seq[CdfAdvance]): Unit =
    if (requests.map(_.name).distinct.size != requests.size) requests.foreach(advanceOne(spark, _))
    else
      OpenIvmMetrics.time("catalog.mv_catalog.advance_batch") {
        withDeltaRetry {
          ensureTables(spark)
          def named(r: CdfAdvance): Column     = col(Name) === lit(r.name)
          def advancing(r: CdfAdvance): Column = named(r) && col(LastVersion) < lit(r.newVersion)
          def byRow(matches: CdfAdvance => Column, value: CdfAdvance => Column, otherwise: Column): Column =
            requests.tail
              .foldLeft(when(matches(requests.head), value(requests.head))) { (acc, r) =>
                acc.when(matches(r), value(r))
              }
              .otherwise(otherwise)
          DeltaTable
            .forPath(spark, path(spark))
            .update(
              col(Name).isin(requests.map(_.name): _*),
              Map(
                LastVersion -> byRow(advancing, r => lit(r.newVersion), col(LastVersion)),
                Properties  -> byRow(named, advancedProperties, col(Properties))
              )
            )
        }
      }

  private def keyed(prefix: String, watermarks: Map[String, Long]): Seq[(String, Long)] =
    watermarks.toSeq.map { case (source, version) => s"$prefix$source" -> version }.sortBy(_._1)

  private def submitAdvance(spark: SparkSession, request: CdfAdvance): Unit =
    DeltaGroupCommit
      .forPath[CdfAdvance](path(spark))
      .submit(request)(advanceOne(spark, _))(advanceBatch(spark, _))

  override def advanceWithCdfWatermarks(
      spark: SparkSession,
      name: TableIdentifier,
      newVersion: Long,
      watermarks: Map[String, Long]
  ): Unit = submitAdvance(spark, CdfAdvance(name, newVersion, keyed(MvMetadata.WatermarkKeyPrefix, watermarks)))

  /** [[advanceWithCdfWatermarks]] that also moves the view's live CDF watermarks forward in the same Delta commit, so the
    * version and the consumed watermarks become durable together. Readers take the greater of these and the
    * `cdf_watermarks` rows.
    */
  private[common] def advanceWithFoldedCdfWatermarks(
      spark: SparkSession,
      name: TableIdentifier,
      newVersion: Long,
      watermarks: Map[String, Long]
  ): Unit = submitAdvance(
    spark,
    CdfAdvance(
      name,
      newVersion,
      keyed(MvMetadata.WatermarkKeyPrefix, watermarks),
      keyed(LiveCdfWatermarkKeyPrefix, watermarks)
    )
  )

  /** Whether the CDF watermarks recorded under `viewName` live in the row stored under `name`, so an advance of it may
    * fold them. Like `cdf_watermarks`, which is keyed by the exact view name, this never resolves the name.
    */
  private[common] def foldsCdfWatermarksOf(name: TableIdentifier, viewName: String): Boolean =
    viewName == serializedName(name)

  /** The live CDF watermarks of `sources` folded into the row named `viewName`, as `(source, version)` over the latest
    * snapshot. None when the catalog does not exist; reading never creates it.
    */
  private[common] def liveCdfWatermarks(
      spark: SparkSession,
      viewName: String,
      sources: Seq[String]
  ): Option[DataFrame] =
    loadIfExists(spark).map { table =>
      val start = LiveCdfWatermarkKeyPrefix.length + 1
      table
        .filter(col(Name) === lit(viewName))
        .select(explode(liveCdfWatermarkProperties(col(Properties))))
        .filter(col("value").startsWith("v:"))
        .select(
          col("key").substr(start, Int.MaxValue).as("source"),
          substring(col("value"), 3, 20).cast(LongType).as("version")
        )
        .filter(col("source").isin(sources: _*))
    }

  private def loadIfExists(spark: SparkSession): Option[DataFrame] =
    try Some(spark.read.format("delta").load(path(spark)))
    catch { case NonFatal(e) => if (catalogExists(spark)) throw e else None }

  private def catalogExists(spark: SparkSession): Boolean = {
    val catalogPath = new org.apache.hadoop.fs.Path(path(spark))
    catalogPath.getFileSystem(spark.sessionState.newHadoopConf()).exists(catalogPath)
  }

  private[common] def removeLiveCdfWatermarksForView(spark: SparkSession, viewName: String): Unit =
    removeLiveCdfWatermarks(spark, col(Name) === lit(viewName), isLiveCdfWatermark)

  private[common] def removeLiveCdfWatermarksForSource(spark: SparkSession, source: String): Unit =
    removeLiveCdfWatermarks(spark, lit(true), _ === lit(s"$LiveCdfWatermarkKeyPrefix$source"))

  private def removeLiveCdfWatermarks(spark: SparkSession, rows: Column, removed: Column => Column): Unit =
    OpenIvmMetrics.time("catalog.mv_catalog.remove_live_cdf_watermarks") {
      if (catalogExists(spark))
        withDeltaRetry {
          DeltaTable
            .forPath(spark, path(spark))
            .update(
              rows && exists(map_keys(col(Properties)), removed),
              Map(Properties -> map_filter(col(Properties), (key, _) => not(removed(key))))
            )
        }
    }

  override def updateProperties(
      spark: SparkSession,
      name: TableIdentifier,
      properties: Map[String, String]
  ): Unit = OpenIvmMetrics.time("catalog.mv_catalog.update_properties") {
    withDeltaRetry {
      ensureTables(spark)
      val updateSchema = StructType(
        Seq(
          StructField(Name, StringType, nullable = false),
          StructField(Properties, MapType(StringType, StringType, valueContainsNull = false), nullable = false)
        )
      )
      val incoming = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(serializedName(name), properties)), 1),
        updateSchema
      )
      DeltaTable
        .forPath(spark, path(spark))
        .as("target")
        .merge(incoming.as("incoming"), col(s"target.$Name") === col(s"incoming.$Name"))
        .whenMatched()
        .update(
          Map(
            Properties -> map_concat(
              visibleProperties(col(s"incoming.$Properties")),
              liveCdfWatermarkProperties(col(s"target.$Properties"))
            )
          )
        )
        .execute()
    }
  }

  override def remove(spark: SparkSession, name: TableIdentifier): Unit =
    OpenIvmMetrics.time("catalog.mv_catalog.remove") {
      withDeltaRetry {
        ensureTables(spark)
        DeltaTable.forPath(spark, path(spark)).delete(col(Name) === lit(serializedName(name)))
      }
    }
}
