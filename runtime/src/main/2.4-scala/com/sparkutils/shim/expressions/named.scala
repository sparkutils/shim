package com.sparkutils.shim.expressions

import org.apache.spark.sql.catalyst.expressions.{Alias, ExprId, Expression, NamedExpression}
import org.apache.spark.sql.types.Metadata

object Alias2 {
  // spark 2.4 and 3.0 only nonInheritableMetadataKeys not used, after 3.1.3 it's stable
  def apply(child: Expression, name: String)(exprId: ExprId = NamedExpression.newExprId, qualifier: scala.Seq[String] = scala.Seq.empty, explicitMetadata: Option[Metadata] = None, nonInheritableMetadataKeys: scala.Seq[String] = scala.Seq.empty): Alias =
    Alias(child, name)(exprId, qualifier, explicitMetadata)

  def unapply(alias: Alias): Option[(Expression, String)] =
    Alias.unapply(alias)
}
