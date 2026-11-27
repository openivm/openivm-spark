package org.openivm.spark.commands

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Alias, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, View}
import org.apache.spark.sql.delta.files.{TahoeFileIndex, TahoeLogFileIndex}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.types.StructType
import org.openivm.spark.common.{ChangeWatermark, DeltaTableVersion, MvMetadata, MvProjectionSource}
import org.openivm.spark.compiler.SparkTimeTravelSql

private[spark] object CdfCreateSnapshots {
  val Property     = "_ivm_cdf_create_snapshots_v1"
  private val json = new ObjectMapper()

  private def path(value: String): String =
    new org.apache.hadoop.fs.Path(value).toUri.normalize().toString.stripSuffix("/")

  final case class Source(
      name: String,
      dataPath: String,
      tableId: String,
      version: Long,
      schema: StructType,
      tracked: Boolean
  )

  final class Captured private[CdfCreateSnapshots] (
      val sources: Seq[Source]
  ) {
    def versions: Map[String, Long] = sources.filter(_.tracked).map(s => s.name -> s.version).toMap

    def properties: Map[String, String] = {
      val array = json.createArrayNode()
      sources.sortBy(_.name).foreach { source =>
        val node = array.addObject()
        node.put("source", source.name)
        node.put("path", source.dataPath)
        node.put("tableId", source.tableId)
        node.put("version", source.version)
        node.put("tracked", source.tracked)
      }
      val encoded = json.writeValueAsString(array)
      require(
        encoded.getBytes(java.nio.charset.StandardCharsets.UTF_8).length <= 65536,
        "CREATE source vector is too large"
      )
      Map(Property -> encoded)
    }

    def verify(spark: SparkSession): Unit = sources.foreach { source =>
      val log = DeltaTableVersion.deltaLogOption(spark, source.name).getOrElse {
        throw new IllegalStateException(s"CREATE source '${source.name}' no longer resolves to a Delta table")
      }
      val snapshot = log.update()
      if (
        path(log.dataPath.toString) != path(source.dataPath) ||
        snapshot.metadata.id != source.tableId || snapshot.version < source.version
      )
        throw new IllegalStateException(s"CREATE source '${source.name}' changed physical Delta identity")
    }

    def initialRows(spark: SparkSession, plan: LogicalPlan): DataFrame = {
      val frames = sources.map { source =>
        val raw = spark.read.format("delta").option("versionAsOf", source.version).load(source.dataPath)
        val indexes = raw.queryExecution.analyzed.collect { case logical: LogicalRelation =>
          logical.relation match {
            case relation: HadoopFsRelation =>
              relation.location match {
                case index: TahoeLogFileIndex => Some(index)
                case _                        => None
              }
            case _ => None
          }
        }.flatten
        require(
          indexes.size == 1 && path(indexes.head.path.toString) == path(source.dataPath) &&
            indexes.head.metadata.id == source.tableId && indexes.head.versionToUse.contains(source.version),
          s"CREATE source '${source.name}' did not bind the captured Delta snapshot"
        )
        val schemaChanged = source.schema.fields.exists { field =>
          val matches = raw.schema.fields.filter(_.name.equalsIgnoreCase(field.name))
          matches.length != 1 || matches.head.dataType != field.dataType
        }
        require(
          !schemaChanged || MvProjectionSource.isProjection(spark, source.name),
          s"CREATE source '${source.name}' schema changed while capturing its snapshot"
        )
        source.name -> MvProjectionSource.project(
          raw,
          source.schema,
          names = MvProjectionSource.outputNames(spark, source.name)
        )
      }.toMap
      val used = scala.collection.mutable.Set.empty[String]
      def replace(original: LogicalPlan, source: Source): LogicalPlan = {
        val replacement = frames(source.name).queryExecution.analyzed
        require(original.output.size == replacement.output.size, s"CREATE source '${source.name}' schema changed")
        val columns = replacement.output.zip(original.output).map { case (read, expected) =>
          require(read.dataType == expected.dataType, s"CREATE source '${source.name}' schema changed")
          Alias(read, expected.name)(
            exprId = expected.exprId,
            qualifier = expected.qualifier,
            explicitMetadata = Some(expected.metadata)
          )
        }
        used += source.name
        Project(columns, replacement)
      }
      def bind(current: LogicalPlan): LogicalPlan = current match {
        case view: View if MvProjectionSource.isProjection(view) =>
          val identifier = view.desc.identifier
          val name       = identifier.database.fold(identifier.table)(db => s"$db.${identifier.table}")
          val matches    = sources.filter(_.name.equalsIgnoreCase(name))
          require(matches.size == 1, s"CREATE public projection '$name' has no unique source snapshot")
          replace(view, matches.head)
        case logical: LogicalRelation =>
          val matches = logical.relation match {
            case relation: HadoopFsRelation =>
              relation.location match {
                case index: TahoeFileIndex =>
                  val atPath = sources.filter(s => path(s.dataPath) == path(index.deltaLog.dataPath.toString))
                  require(
                    atPath.isEmpty || atPath.forall(_.tableId == index.metadata.id),
                    "CREATE analyzed source changed physical Delta identity"
                  )
                  atPath
                case _ => Nil
              }
            case _ => Nil
          }
          if (matches.isEmpty) logical
          else {
            val catalogName = logical.catalogTable.map { table =>
              table.identifier.database.fold(table.identifier.table)(db => s"$db.${table.identifier.table}")
            }
            val named      = matches.filter(source => catalogName.exists(_.equalsIgnoreCase(source.name)))
            val candidates = if (named.nonEmpty) named else matches
            require(
              candidates.map(source => (source.version, source.schema)).distinct.size == 1,
              "CREATE source has conflicting snapshot versions or schemas"
            )
            used ++= candidates.map(_.name)
            replace(logical, candidates.head)
          }
        case other =>
          other.mapChildren(bind).transformExpressionsDown { case sub: SubqueryExpression =>
            sub.withNewPlan(bind(sub.plan))
          }
      }
      val bound = bind(plan)
      require(sources.forall(s => used.contains(s.name)), "CREATE initial plan did not bind every source snapshot")
      org.apache.spark.sql.openivm.StreamingDatasetAccess.ofRows(spark, bound)
    }
  }

  def verifyRecovered(spark: SparkSession, properties: Map[String, String], sourceNames: Seq[String]): Unit =
    properties.get(Property).foreach { encoded =>
      require(
        encoded.getBytes(java.nio.charset.StandardCharsets.UTF_8).length <= 65536,
        "CREATE source vector is too large"
      )
      val nodes = json.readTree(encoded)
      require(nodes != null && nodes.isArray, "invalid CREATE source vector")
      val records = (0 until nodes.size()).map(i => nodes.get(i))
      val names = records.map { node =>
        require(
          node.isObject && node.hasNonNull("source") && node.get("source").isTextual &&
            node.hasNonNull("path") && node.get("path").isTextual &&
            node.hasNonNull("tableId") && node.get("tableId").isTextual &&
            node
              .hasNonNull("version") && node.get("version").isIntegralNumber && node.get("version").canConvertToLong &&
            node.hasNonNull("tracked") && node.get("tracked").isBoolean,
          "invalid CREATE source identity"
        )
        val name    = node.get("source").asText()
        val version = node.get("version").asLong()
        val tracked = node.get("tracked").asBoolean()
        val log = DeltaTableVersion.deltaLogOption(spark, name).getOrElse {
          throw new IllegalStateException(s"Recovered CREATE source '$name' no longer resolves to a Delta table")
        }
        val snapshot = log.update()
        require(
          path(log.dataPath.toString) == path(node.get("path").asText()) &&
            snapshot.metadata.id == node.get("tableId").asText() && version >= 0L && snapshot.version >= version &&
            (!tracked || properties
              .get(s"${MvMetadata.WatermarkKeyPrefix}$name")
              .contains(ChangeWatermark.DeltaVersion(version).encode)),
          s"Recovered CREATE source '$name' does not match its saved physical snapshot"
        )
        name -> tracked
      }
      require(
        names.map(_._1).distinct.size == names.size && names.collect { case (name, true) =>
          name
        }.toSet == sourceNames.toSet,
        "CREATE source vector changed"
      )
    }

  def capture(
      spark: SparkSession,
      sourceNames: Seq[String],
      schemas: Map[String, StructType],
      pins: Seq[SparkTimeTravelSql.ResolvedSnapshotPin],
      analyzedInitial: LogicalPlan
  ): Captured = {
    def scans(plan: LogicalPlan): Seq[(LogicalRelation, TahoeFileIndex)] = {
      val local = plan match {
        case logical: LogicalRelation =>
          logical.relation match {
            case relation: HadoopFsRelation =>
              relation.location match {
                case index: TahoeFileIndex => Seq(logical -> index)
                case _                     => Nil
              }
            case _ => Nil
          }
        case _ => Nil
      }
      local ++ (plan.children ++ plan.expressions.flatMap(_.collect { case sub: SubqueryExpression => sub.plan }))
        .flatMap(scans)
    }
    val allScans = scans(analyzedInitial)
    val pinnedScans = allScans.flatMap { case (_, index) =>
      index match {
        case timeTravel: TahoeLogFileIndex =>
          timeTravel.versionToUse.toSeq.map(version => path(timeTravel.path.toString) -> version)
        case _ => Nil
      }
    }
    val trackedSources = sourceNames.distinct.map { name =>
      val log = DeltaTableVersion.deltaLogOption(spark, name).getOrElse {
        throw new IllegalStateException(s"CREATE source '$name' has no readable Delta log")
      }
      val snapshot = log.update()
      require(snapshot.version >= 0L, s"CREATE source '$name' has no committed Delta snapshot")
      val sourcePins = pins.filter(_.emitsResolved == name)
      sourcePins.foreach { pin =>
        require(
          path(pin.operationalSource.deltaLogDataPath) == path(log.dataPath.toString) &&
            pin.operationalSource.deltaTableMetadataId == snapshot.metadata.id,
          s"CREATE pinned source '$name' changed physical Delta identity"
        )
      }
      val version =
        if (sourcePins.isEmpty) snapshot.version
        else {
          val versions = pinnedScans.collect {
            case (scanPath, v) if scanPath == path(log.dataPath.toString) => v
          }.distinct
          require(versions.size == 1, s"CREATE pinned source '$name' has no uniquely resolved physical snapshot")
          versions.head
        }
      Source(name, log.dataPath.toString, snapshot.metadata.id, version, schemas(name), tracked = true)
    }
    val untracked = allScans
      .filterNot { case (_, index) =>
        trackedSources.exists(s => path(s.dataPath) == path(index.deltaLog.dataPath.toString))
      }
      .groupBy { case (_, index) => path(index.deltaLog.dataPath.toString) }
      .toSeq
      .map { case (_, entries) =>
        val (logical, index) = entries.head
        val snapshot         = index.deltaLog.update()
        val versions = entries.map { case (_, scan) =>
          require(scan.metadata.id == snapshot.metadata.id, "CREATE untracked source changed physical Delta identity")
          scan match {
            case timeTravel: TahoeLogFileIndex => timeTravel.versionToUse.getOrElse(snapshot.version)
            case _                             => snapshot.version
          }
        }.distinct
        require(versions.size == 1, "CREATE untracked source has conflicting snapshot versions")
        val dataPath = index.deltaLog.dataPath.toString
        Source(dataPath, dataPath, snapshot.metadata.id, versions.head, logical.schema, tracked = false)
      }
    new Captured(trackedSources ++ untracked)
  }
}
