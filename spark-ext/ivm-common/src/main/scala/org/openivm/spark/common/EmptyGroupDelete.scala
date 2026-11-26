package org.openivm.spark.common

import java.util.Locale

/** Recognises an aggregate refresh program that already deletes its emptied groups.
  *
  * OpenIVM ends an aggregate program with
  * `DELETE FROM <data> WHERE COALESCE(<c1>, 0) = 0 AND COALESCE(<c2>, 0) = 0 ...` over the view's
  * COUNT-type columns. Each of them counts a subset of the rows that the group's COUNT(*) column
  * counts, so when that COUNT(*) column is one of the conjuncts the statement removes every row
  * whose COUNT(*) is zero. If no later statement writes the target, a second
  * `DELETE ... WHERE <count> = 0` scans the target and finds nothing to remove.
  */
private[spark] object EmptyGroupDelete {

  private val Ident        = """(?:`(?:``|[^`])+`|[A-Za-z_][A-Za-z0-9_]*)"""
  private val QualifiedRef = s"""($Ident(?:\\s*\\.\\s*$Ident)*)"""
  private val DeleteRe     = s"""(?is)^\\s*DELETE\\s+FROM\\s+$QualifiedRef\\s+WHERE\\s+(.+?)\\s*;?\\s*$$""".r
  private val WriteRe =
    s"""(?is)\\b(?:MERGE\\s+INTO|INSERT\\s+INTO|INSERT\\s+OVERWRITE(?:\\s+TABLE)?|UPDATE|DELETE\\s+FROM)\\s+$QualifiedRef""".r
  private val ConjunctRe = s"""(?is)^\\(?\\s*COALESCE\\s*\\(\\s*($Ident)\\s*,\\s*0\\s*\\)\\s*=\\s*0\\s*\\)?$$""".r
  private val AndRe      = """(?i)\s+AND\s+""".r

  /** True when the last statement of `statements` that writes `targetTable` is an emptied-group
    * delete whose conjuncts include `countColumn`.
    *
    * @param statements  rewritten refresh program, in execution order
    * @param targetTable unqualified name of the table the program maintains
    * @param countColumn column holding the group's COUNT(*)
    */
  def programDeletesEmptyGroups(statements: Seq[String], targetTable: String, countColumn: String): Boolean = {
    val target = normalize(targetTable)
    statements
      .map(SparkRefreshRewriter.stripExecutionMarker)
      .filter(sql => WriteRe.findAllMatchIn(sql).exists(m => lastPart(m.group(1)) == target))
      .lastOption
      .exists(sql => emptyGroupColumns(sql, target).contains(normalize(countColumn)))
  }

  private def emptyGroupColumns(sql: String, target: String): Set[String] =
    DeleteRe.findFirstMatchIn(sql.trim) match {
      case Some(m) if lastPart(m.group(1)) == target =>
        val columns = AndRe.split(m.group(2).trim).toSeq.map(c => ConjunctRe.findFirstMatchIn(c.trim).map(_.group(1)))
        if (columns.forall(_.isDefined)) columns.flatten.map(normalize).toSet else Set.empty
      case _ => Set.empty
    }

  private def lastPart(ref: String): String =
    normalize(ref.split("""\s*\.\s*(?=(?:[^`]*`[^`]*`)*[^`]*$)""").last)

  private def normalize(ident: String): String = {
    val trimmed = ident.trim
    val unquoted =
      if (trimmed.length >= 2 && trimmed.startsWith("`") && trimmed.endsWith("`"))
        trimmed.substring(1, trimmed.length - 1).replace("``", "`")
      else trimmed
    unquoted.toLowerCase(Locale.ROOT)
  }
}
