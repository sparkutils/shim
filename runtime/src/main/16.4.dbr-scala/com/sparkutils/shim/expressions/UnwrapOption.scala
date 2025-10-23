package com.sparkutils.shim.expressions

import org.apache.spark.sql.catalyst.expressions.objects.UnwrapOption
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.DataType


// stable enough
object UnwrapOption2 {
  def apply(dataType: DataType,
            child: Expression): UnwrapOption =
    new UnwrapOption(dataType, child)

  def unapply(cast: UnwrapOption): Option[(DataType, Expression)] =
    cast match {
      case UnwrapOption(dataType, child) =>
        Some((dataType, child))
      case _ => None
    }
}
