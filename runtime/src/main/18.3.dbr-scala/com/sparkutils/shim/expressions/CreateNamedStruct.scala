package com.sparkutils.shim.expressions

import org.apache.spark.sql.catalyst.expressions.{Expression, CreateNamedStruct}


/*
2.4
GetStructField(children: Seq[Expression])
3.5
GetStructField(children: Seq[Expression])

 */

// stable enough
object CreateNamedStruct1 {
  def apply(children: Seq[Expression]): CreateNamedStruct =
    new CreateNamedStruct(children)

  def unapply(cast: CreateNamedStruct): Option[(Seq[Expression])] =
    cast match {
      case CreateNamedStruct(children) =>
        Some((children))
      case _ => None
    }
}