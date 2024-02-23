package com.sparkutils.shim.expressions

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * Identifier is always a String (2.4 and 3 have a type)
 */
object UnresolvedFunction5 {

  /**
   * Creates nameParts based on splitting "."
   * @param nameParts
   * @param arguments
   * @param isDistinct
   * @param filter
   * @param ignoreNulls
   * @return
   */
  def apply(
    nameParts: String,
    arguments: Seq[Expression],
    isDistinct: Boolean,
    filter: Option[Expression] = None,
    ignoreNulls: Boolean = false) = UnresolvedFunction(FunctionIdentifier(nameParts), arguments, isDistinct, filter)

  def unapply(unresolvedFunction: UnresolvedFunction): Option[(String, Seq[Expression], Boolean, Option[Expression], Boolean)] =
    unresolvedFunction match {
      case u@UnresolvedFunction(_, argumentExpressions, is, filter) =>
        Some((Names.toName(u), argumentExpressions, is, filter, false))
      case _ => None
    }

}

object UnresolvedFunction4 {

  /**
   * Creates nameParts based on splitting "."
   * @param nameParts
   * @param arguments
   * @param isDistinct
   * @param filter
   * @param ignoreNulls
   * @return
   */
  def apply(
             nameParts: String,
             arguments: Seq[Expression],
             isDistinct: Boolean,
             filter: Option[Expression] = None) = UnresolvedFunction(FunctionIdentifier(nameParts), arguments, isDistinct, filter)

  def unapply(unresolvedFunction: UnresolvedFunction): Option[(String, Seq[Expression], Boolean, Option[Expression])] =
    unresolvedFunction match {
      case u@UnresolvedFunction(_, argumentExpressions, is, filter) =>
        Some((Names.toName(u), argumentExpressions, is, filter))
      case _ => None
    }

}
