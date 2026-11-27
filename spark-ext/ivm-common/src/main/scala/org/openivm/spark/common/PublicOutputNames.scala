package org.openivm.spark.common

import java.util.Locale

import scala.util.Try

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedHaving}
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical._

/** Public output names of an MV whose user aliases fall in native OpenIVM's reserved `openivm_` namespace.
  *
  * Native OpenIVM rejects explicit output aliases starting with `openivm_` (openivm/openivm#193) because its
  * column hider owns that namespace. Spark MVs keep accepting such user names: the compiled query and backing
  * table use distinct physical names, and the public view projects every physical column back to its user name.
  * Pairs are `(public, physical)`.
  */
final case class PublicOutputNames(pairs: Seq[(String, String)]) {
  def isEmpty: Boolean  = pairs.isEmpty
  def nonEmpty: Boolean = pairs.nonEmpty

  def physical(public: String): String =
    pairs.collectFirst { case (p, phys) if p.equalsIgnoreCase(public) => phys }.getOrElse(public)

  def publicName(physical: String): String =
    pairs.collectFirst { case (p, phys) if phys.equalsIgnoreCase(physical) => p }.getOrElse(physical)

  def physicalFor(public: String): Option[String] =
    pairs.collectFirst { case (p, phys) if p.equalsIgnoreCase(public) => phys }

  def properties: Map[String, String] =
    if (isEmpty) Map.empty
    else Map(PublicOutputNames.CatalogProperty -> PublicOutputNames.encode(pairs))
}

object PublicOutputNames {
  val CatalogProperty: String  = "_ivm_public_output_names_v1"
  val Empty: PublicOutputNames = PublicOutputNames(Nil)

  private val ReservedPrefix  = "openivm_"
  private val PhysicalPrefix  = "ivm_alias_"
  private val NativeAppended  = Set("openivm_multiplicity", "openivm_timestamp")
  private val Json            = new ObjectMapper()
  private val VerifyAttempts  = 3
  private val IdentifierToken = "[A-Za-z0-9_]+".r

  final case class Plan(physicalSql: String, names: PublicOutputNames)

  def of(properties: Map[String, String]): PublicOutputNames =
    properties.get(CatalogProperty).fold(Empty)(decode)

  private def encode(pairs: Seq[(String, String)]): String = {
    val array = Json.createArrayNode()
    pairs.foreach { case (public, physical) => array.addArray().add(public).add(physical) }
    Json.writeValueAsString(array)
  }

  private def decode(text: String): PublicOutputNames = {
    val node = Json.readTree(text)
    require(node.isArray, s"Malformed $CatalogProperty")
    PublicOutputNames((0 until node.size()).map { i =>
      val pair = node.get(i)
      require(pair.isArray && pair.size() == 2, s"Malformed $CatalogProperty entry")
      pair.get(0).asText() -> pair.get(1).asText()
    })
  }

  private def reserved(name: String): Boolean = {
    val lower = name.toLowerCase(Locale.ROOT)
    lower.startsWith(ReservedPrefix) && !NativeAppended(lower)
  }

  private def outputs(plan: LogicalPlan): Seq[NamedExpression] = plan match {
    case p: Project              => p.projectList
    case a: Aggregate            => a.aggregateExpressions
    case u: Union if u.byName    => u.children.flatMap(outputs)
    case u: Union                => u.children.headOption.toSeq.flatMap(outputs)
    case e: Except               => outputs(e.left)
    case i: Intersect            => outputs(i.left)
    case w: UnresolvedWith       => outputs(w.child)
    case s: Sort                 => outputs(s.child)
    case d: Distinct             => outputs(d.child)
    case l: GlobalLimit          => outputs(l.child)
    case l: LocalLimit           => outputs(l.child)
    case o: Offset               => outputs(o.child)
    case f: Filter               => outputs(f.child)
    case h: UnresolvedHaving     => outputs(h.child)
    case w: WithWindowDefinition => outputs(w.child)
    case _                       => Nil
  }

  /** Top-level ORDER BY, HAVING and GROUP BY references that may name an output alias. */
  private def aliasReferences(plan: LogicalPlan): Seq[Expression] = plan match {
    case s: Sort                 => s.order ++ aliasReferences(s.child)
    case h: UnresolvedHaving     => h.havingCondition +: aliasReferences(h.child)
    case a: Aggregate            => a.groupingExpressions
    case w: UnresolvedWith       => aliasReferences(w.child)
    case d: Distinct             => aliasReferences(d.child)
    case l: GlobalLimit          => aliasReferences(l.child)
    case l: LocalLimit           => aliasReferences(l.child)
    case o: Offset               => aliasReferences(o.child)
    case f: Filter               => aliasReferences(f.child)
    case w: WithWindowDefinition => aliasReferences(w.child)
    case _                       => Nil
  }

  private def identifierPart(c: Char): Boolean = Character.isLetterOrDigit(c) || c == '_'

  /** Span `[begin, end)` of `name` ending an expression whose origin is `[start, stop]`, quoted or bare. */
  private def nameSpan(sql: String, name: String, start: Option[Int], stop: Option[Int]): Option[(Int, Int)] =
    (start, stop) match {
      case (Some(s), Some(e)) if s >= 0 && s <= e && e < sql.length =>
        val quoted = "`" + name + "`"
        Seq(e, e + 1)
          .collectFirst {
            case close
                if close < sql.length && sql.charAt(close) == '`' && close + 1 - quoted.length >= s &&
                  sql.startsWith(quoted, close + 1 - quoted.length) =>
              (close + 1 - quoted.length, close + 1)
          }
          .orElse(
            Some(e + 1 - name.length)
              .filter { begin =>
                begin >= s && sql.startsWith(name, begin) &&
                (begin == 0 || !identifierPart(sql.charAt(begin - 1))) &&
                (e + 1 == sql.length || !identifierPart(sql.charAt(e + 1)))
              }
              .map(begin => (begin, e + 1))
          )
      case _ => None
    }

  private def physicalName(ordinal: Int, public: String, taken: String => Boolean): String = {
    val rest = public.drop(ReservedPrefix.length).toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_]", "_")
    val base =
      if (rest.isEmpty || rest.contains("openivm")) s"$PhysicalPrefix$ordinal" else s"$PhysicalPrefix${ordinal}_$rest"
    Iterator.from(1).map(n => if (n == 1) base else s"${base}_$n").find(candidate => !taken(candidate)).get
  }

  /** Physical compile text and public-name map for `sql`, or `None` when no output needs mapping or the
    * rewrite cannot be proven to change only output names (the native rejection then applies unchanged).
    */
  def plan(spark: SparkSession, sql: String): Option[Plan] = {
    val parsed = Try(CatalystSqlParser.parsePlan(sql)).toOption
    parsed.flatMap { tree =>
      val aliases = outputs(tree).collect { case alias: Alias if reserved(alias.name) => alias }
      if (aliases.isEmpty) None
      else Try(build(spark, sql, tree, aliases)).toOption.flatten
    }
  }

  private def build(spark: SparkSession, sql: String, tree: LogicalPlan, aliases: Seq[Alias]): Option[Plan] = {
    val original    = spark.sql(sql).queryExecution.analyzed
    val outputNames = original.output.map(_.name)
    val duplicateOutputs =
      outputNames.groupBy(_.toLowerCase(Locale.ROOT)).exists(_._2.size > 1)
    if (duplicateOutputs) return None
    val knownNames: Set[String] =
      (IdentifierToken.findAllIn(sql).toSeq ++
        original.collectWithSubqueries { case node => node.output.map(_.name) }.flatten)
        .map(_.toLowerCase(Locale.ROOT))
        .toSet
    val assigned = scala.collection.mutable.LinkedHashMap.empty[String, (String, String)]
    aliases.foreach { alias =>
      val key = alias.name.toLowerCase(Locale.ROOT)
      if (!assigned.contains(key)) {
        val ordinal = outputNames.indexWhere(_.equalsIgnoreCase(alias.name)) + 1
        val taken = (candidate: String) =>
          knownNames(candidate) || assigned.valuesIterator.exists(_._2.equalsIgnoreCase(candidate))
        assigned(key) = alias.name -> physicalName(math.max(ordinal, 1), alias.name, taken)
      }
    }
    val physicalOf = assigned.map { case (key, (_, physical)) => key -> physical }.toMap
    val aliasEdits = aliases.map { alias =>
      nameSpan(sql, alias.name, alias.origin.startIndex, alias.origin.stopIndex)
        .map(span => (span, physicalOf(alias.name.toLowerCase(Locale.ROOT))))
    }
    if (aliasEdits.exists(_.isEmpty)) return None
    val referenceEdits = aliasReferences(tree).flatMap { expression =>
      val found = scala.collection.mutable.ArrayBuffer.empty[Option[((Int, Int), String)]]
      expression.foreach {
        case attr: UnresolvedAttribute if attr.nameParts.size == 1 =>
          physicalOf.get(attr.nameParts.head.toLowerCase(Locale.ROOT)).foreach { physical =>
            found += nameSpan(sql, attr.nameParts.head, attr.origin.startIndex, attr.origin.stopIndex)
              .map(_ -> physical)
          }
        case _ =>
      }
      found
    }
    if (referenceEdits.exists(_.isEmpty)) return None
    val expected = outputNames.map(name => physicalOf.getOrElse(name.toLowerCase(Locale.ROOT), name))
    val names    = PublicOutputNames(assigned.values.toSeq)
    Seq(aliasEdits.flatten ++ referenceEdits.flatten, aliasEdits.flatten).distinct.iterator
      .map(edits => rewrite(sql, edits))
      .collectFirst { case Some(rewritten) if verified(spark, sql, rewritten, expected) => Plan(rewritten, names) }
  }

  private def rewrite(sql: String, edits: Seq[((Int, Int), String)]): Option[String] = {
    val ordered  = edits.distinct.sortBy(-_._1._1)
    val overlaps = ordered.zip(ordered.drop(1)).exists { case (later, earlier) => earlier._1._2 > later._1._1 }
    if (overlaps) None
    else {
      val builder = new StringBuilder(sql)
      ordered.foreach { case ((begin, end), physical) => builder.replace(begin, end, physical) }
      Some(builder.toString)
    }
  }

  /** Canonicalized plans erase alias and attribute names, so equality proves only names changed. */
  private def verified(spark: SparkSession, sql: String, rewritten: String, expected: Seq[String]): Boolean =
    (1 to VerifyAttempts).exists { _ =>
      Try {
        val before = spark.sql(sql).queryExecution.analyzed
        val after  = spark.sql(rewritten).queryExecution.analyzed
        after.output.map(_.name) == expected &&
        after.output.map(_.dataType) == before.output.map(_.dataType) &&
        after.sameResult(before)
      }.getOrElse(false)
    }
}
