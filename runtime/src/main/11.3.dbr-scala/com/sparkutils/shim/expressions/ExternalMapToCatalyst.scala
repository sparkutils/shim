package com.sparkutils.shim.expressions

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.ExternalMapToCatalyst
import org.apache.spark.sql.types.DataType

// stable from 2.4 through to 3.5
object ExternalMapToCatalyst7 {
  def apply(
             inputMap: Expression,
             keyType: DataType,
             keyConverter: Expression => Expression,
             keyNullable: Boolean,
             valueType: DataType,
             valueConverter: Expression => Expression,
             valueNullable: Boolean): ExternalMapToCatalyst =
    ExternalMapToCatalyst(inputMap, keyType, keyConverter, keyNullable, valueType, valueConverter, valueNullable)
}