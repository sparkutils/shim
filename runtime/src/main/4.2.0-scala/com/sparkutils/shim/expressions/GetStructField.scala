package com.sparkutils.shim.expressions

import org.apache.spark.sql.catalyst.expressions.{Expression, GetStructField}

/*
2.4
GetStructField(child: Expression, ordinal: Int, name: Option[String] = None)
3.5
GetStructField(child: Expression, ordinal: Int, name: Option[String] = None)

 */

// stable enough
object GetStructField3 {
  def apply(child: Expression, ordinal: Int, name: Option[String] = None): GetStructField =
    new GetStructField(child, ordinal, name)

  def unapply(cast: GetStructField): Option[(Expression, Int, Option[String])] =
    cast match {
      case GetStructField(child, ordinal, name) =>
        Some((child, ordinal, name))
      case _ => None
    }
}