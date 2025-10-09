package com.sparkutils.shim.expressions

import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.apache.spark.sql.catalyst.expressions.{NamedExpression, UnresolvedNamedLambdaVariable}

/**
 * Hides differences in name usage
 */
object Names {

  def toName(ne: NamedExpression): String =
    ne match {
      case nv: UnresolvedNamedLambdaVariable => toName(nv)
      case _ => toName(ne.qualifier :+ ne.name)
    }

  def toName(nv: UnresolvedNamedLambdaVariable): String =
    toName(nv.nameParts)

  def toName(parts: Seq[String]): String =
    parts.mkString(".")

  def toName(unresolvedFunction: UnresolvedFunction): String =
    toName(unresolvedFunction.nameParts)

}
