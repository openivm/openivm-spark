package org.openivm.spark.common.rocksdb

import org.rocksdb.RocksDB
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Path}
import java.security.MessageDigest
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

private[rocksdb] object OpenIvmCheckpointReuse {
  final case class Identity(
      handle: RocksDB,
      directoryKey: Object,
      sequence: Long,
      columnFamilies: Set[String],
      manifests: Vector[(String, String)]
  )

  private final case class FileCopy(name: String, bytes: Array[Byte], digest: Vector[Byte])
  private final case class Entry(identity: Identity, files: Vector[FileCopy], bytes: Long)
  private val log = LoggerFactory.getLogger(getClass)
  private val maxBytes = 128L * 1024L * 1024L
  private val maxEntries = 256
  private val maxStoreBytes = 16L * 1024L * 1024L
  private val cache = mutable.LinkedHashMap.empty[OpenIvmRocksDB, Entry]
  private var retainedBytes = 0L

  private def hash(bytes: Array[Byte]): Vector[Byte] = MessageDigest.getInstance("SHA-256").digest(bytes).toVector

  private def remove(db: OpenIvmRocksDB): Unit =
    cache.remove(db).foreach(entry => retainedBytes -= entry.bytes)

  private def readCheckpoint(path: Path, identity: Identity): Option[Entry] = {
    val paths = Files.walk(path)
    try {
      val files = paths.iterator().asScala.toVector.filter(_ != path)
      if (files.exists(Files.isSymbolicLink(_))) throw new java.io.IOException("symbolic checkpoint entry")
      val regular = files.filter(Files.isRegularFile(_)).sortBy(_.toString)
      val size = regular.map(Files.size(_)).sum
      if (size > maxStoreBytes) None
      else {
        val copies = regular.map { file =>
          OpenIvmSnapshotArchive.checkInterrupted()
          val bytes = Files.readAllBytes(file)
          FileCopy(path.relativize(file).toString, bytes, hash(bytes))
        }
        val actualSize = copies.map(_.bytes.length.toLong).sum
        if (actualSize > maxStoreBytes) None else Some(Entry(identity, copies, actualSize))
      }
    } finally paths.close()
  }

  private def retain(db: OpenIvmRocksDB, path: Path, identity: Identity): Unit = {
    remove(db)
    try {
      readCheckpoint(path, identity).foreach { entry =>
        while (cache.nonEmpty && (cache.size >= maxEntries || retainedBytes + entry.bytes > maxBytes))
          remove(cache.head._1)
        cache.put(db, entry)
        retainedBytes += entry.bytes
      }
    } catch {
      case interrupted: java.io.InterruptedIOException => throw interrupted
      case NonFatal(error) => log.warn("openivm state-sync: could not retain checkpoint for reuse", error)
    }
  }

  private def copy(entry: Entry, destination: Path): Boolean = {
    if (entry.files.exists(file => hash(file.bytes) != file.digest)) false
    else {
      Files.createDirectories(destination)
      entry.files.foreach { file =>
        OpenIvmSnapshotArchive.checkInterrupted()
        val target = destination.resolve(file.name)
        Files.createDirectories(target.getParent)
        Files.write(target, file.bytes)
      }
      true
    }
  }

  def checkpoint(db: OpenIvmRocksDB, destination: Path): String =
    db.withCheckpointIdentity { identity =>
      synchronized {
        val previous = cache.get(db)
        previous match {
          case Some(entry) if entry.identity == identity && copy(entry, destination) =>
            remove(db)
            cache.put(db, entry)
            retainedBytes += entry.bytes
            "reuse"
          case _ =>
            val outcome = if (previous.isDefined) "fallback" else "fresh"
            db.checkpointTo(destination)
            retain(db, destination, identity)
            outcome
        }
      }
    }.getOrElse {
      db.checkpointTo(destination)
      "fallback"
    }
}
