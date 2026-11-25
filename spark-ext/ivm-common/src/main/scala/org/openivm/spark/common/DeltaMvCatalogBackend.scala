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
  greatest,
  lit,
  map_concat,
  map_filter,
  map_from_arrays,
  not,
  substring,
  when
}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.openivm.spark.telemetry.metrics.OpenIvmMetrics

import scala.jdk.CollectionConverters._

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

  private def pathAt(catalogPath: String): String = catalogPath.stripSuffix("/") + "/mv_metadata"

  private def path(spark: SparkSession): String = pathAt(FeatureGate.catalogPath(spark))

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
      properties = row.getJavaMap[String, String](row.fieldIndex(Properties)).asScala.toMap
    )

  override def ensureTables(spark: SparkSession): Unit = OpenIvmMetrics.time("catalog.mv_catalog.ensure_tables") {
    withDeltaRetry {
      DeltaPathTable.createIfAbsent(spark, path(spark), schema, Name)
    }
  }

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
        .updateAll()
        .whenNotMatched()
        .insertAll()
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

  private final case class CdfAdvance(id: TableIdentifier, newVersion: Long, entries: Seq[(String, Long)]) {
    val name: String = serializedName(id)
  }

  private def withCdfWatermarks(entries: Seq[(String, Long)]): Column = {
    val keys       = array(entries.map { case (key, _) => lit(key) }: _*)
    val properties = col(Properties)
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

  private def advanceOne(spark: SparkSession, request: CdfAdvance): Unit =
    if (request.entries.isEmpty)
      advance(spark, request.id, request.newVersion)
    else
      withDeltaRetry {
        ensureTables(spark)
        DeltaTable
          .forPath(spark, path(spark))
          .update(
            col(Name) === lit(request.name) && col(LastVersion) < lit(request.newVersion),
            Map(LastVersion -> lit(request.newVersion), Properties -> withCdfWatermarks(request.entries))
          )
      }

  /** One Delta UPDATE that applies each request's single-view advance to its own row (rows are keyed by name).
    *
    * The condition names only `name` partitions, so Delta rewrites those rows in one pass instead of first scanning
    * them for matches. The per-row advance condition moves into the assignments: a row whose stored version is not
    * older keeps its values.
    */
  private def advanceBatch(spark: SparkSession, requests: Seq[CdfAdvance]): Unit =
    if (requests.map(_.name).distinct.size != requests.size) requests.foreach(advanceOne(spark, _))
    else
      OpenIvmMetrics.time("catalog.mv_catalog.advance_batch") {
        withDeltaRetry {
          ensureTables(spark)
          def advancing(r: CdfAdvance): Column = col(Name) === lit(r.name) && col(LastVersion) < lit(r.newVersion)
          def byName(value: CdfAdvance => Column, otherwise: Column): Column =
            requests.tail
              .foldLeft(when(advancing(requests.head), value(requests.head))) { (acc, r) =>
                acc.when(advancing(r), value(r))
              }
              .otherwise(otherwise)
          DeltaTable
            .forPath(spark, path(spark))
            .update(
              col(Name).isin(requests.map(_.name): _*),
              Map(
                LastVersion -> byName(r => lit(r.newVersion), col(LastVersion)),
                Properties -> byName(
                  r => if (r.entries.isEmpty) col(Properties) else withCdfWatermarks(r.entries),
                  col(Properties)
                )
              )
            )
        }
      }

  override def advanceWithCdfWatermarks(
      spark: SparkSession,
      name: TableIdentifier,
      newVersion: Long,
      watermarks: Map[String, Long]
  ): Unit =
    DeltaGroupCommit
      .forPath[CdfAdvance](path(spark))
      .submit(CdfAdvance(name, newVersion, watermarkEntries(watermarks)))(advanceOne(spark, _))(advanceBatch(spark, _))

  private def watermarkEntries(watermarks: Map[String, Long]): Seq[(String, Long)] =
    watermarks.toSeq
      .map { case (source, version) =>
        s"${MvMetadata.WatermarkKeyPrefix}$source" -> version
      }
      .sortBy(_._1)

  /** Rolls each row forward to its flushed refresh record, matching name, location and Delta table id. */
  private[common] def advanceRecorded(
      spark: SparkSession,
      catalogPath: String,
      records: Seq[MvRefreshRecords.Pending]
  ): Unit = if (records.nonEmpty) OpenIvmMetrics.time("catalog.mv_catalog.advance") {
    def recorded(record: MvRefreshRecords.Pending): Column =
      col(Name) === lit(record.storedName) && col(Location) === lit(record.location) &&
        col(Properties).getItem(MvMetadata.TargetDeltaTableIdKey) === lit(record.tableId) &&
        col(LastVersion) < lit(record.version)
    def properties(record: MvRefreshRecords.Pending): Column = {
      val entries = watermarkEntries(record.consumed)
      if (entries.isEmpty) col(Properties) else withCdfWatermarks(entries)
    }
    withDeltaRetry {
      DeltaTable
        .forPath(spark, pathAt(catalogPath))
        .update(
          records.map(recorded).reduce(_ || _),
          Map(
            LastVersion -> records.foldRight(col(LastVersion)) { (record, otherwise) =>
              when(recorded(record), lit(record.version)).otherwise(otherwise)
            },
            Properties -> records.foldRight(col(Properties)) { (record, otherwise) =>
              when(recorded(record), properties(record)).otherwise(otherwise)
            }
          )
        )
    }
  }

  /** Stored location, Delta table id and version of each named row; empty when the catalog does not exist. */
  private[common] def storedVersions(
      spark: SparkSession,
      catalogPath: String,
      names: Seq[String]
  ): Map[String, (String, Option[String], Long)] = {
    val target = new org.apache.hadoop.fs.Path(pathAt(catalogPath))
    if (names.isEmpty || !target.getFileSystem(spark.sessionState.newHadoopConf()).exists(target)) Map.empty
    else
      spark.read
        .format("delta")
        .load(pathAt(catalogPath))
        .filter(col(Name).isin(names.distinct: _*))
        .select(col(Name), col(Location), col(Properties).getItem(MvMetadata.TargetDeltaTableIdKey), col(LastVersion))
        .collect()
        .map(row => row.getString(0) -> ((row.getString(1), Option(row.getString(2)), row.getLong(3))))
        .toMap
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
        .update(Map(Properties -> col(s"incoming.$Properties")))
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
