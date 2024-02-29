package com.sparkutils.shim.expressions

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.WrapOption
import org.apache.spark.sql.types.DataType

// stable enough
object WrapOption2 {
  def apply(child: Expression,
            dataType: DataType): WrapOption =
    new WrapOption(child, dataType)

  def unapply(cast: WrapOption): Option[(Expression, DataType)] =
    cast match {
      case WrapOption(child, dataType) =>
        Some((child, dataType))
      case _ => None
    }
}