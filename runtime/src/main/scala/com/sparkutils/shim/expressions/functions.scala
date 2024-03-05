package com.sparkutils.shim.expressions

import org.apache.spark.sql.catalyst.expressions.{Coalesce, Expression}

/*
 * Collection of shims on really stable interfaces for functional expressions
 */

object Coalesce1 {
  def apply(seq: scala.collection.immutable.Seq[Expression]): Coalesce =
    Coalesce(seq)

  def unapply(coalesce: Coalesce): Option[scala.collection.immutable.Seq[Expression]] =
    Coalesce.unapply(coalesce)
}