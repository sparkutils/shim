package com.sparkutils.shim.expressions

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.MapObjects
import org.apache.spark.sql.types.DataType

// stable from 2.4 through to 3.5
object MapObjects5 {
  /**
   * Construct an instance of MapObjects case class.
   *
   * @param function The function applied on the collection elements.
   * @param inputData An expression that when evaluated returns a collection object.
   * @param elementType The data type of elements in the collection.
   * @param elementNullable When false, indicating elements in the collection are always
   *                        non-null value.
   * @param customCollectionCls Class of the resulting collection (returning ObjectType)
   *                            or None (returning ArrayType)
   */
  def apply(
             function: Expression => Expression,
             inputData: Expression,
             elementType: DataType,
             elementNullable: Boolean = true,
             customCollectionCls: Option[Class[_]] = None): MapObjects =
    MapObjects(function, inputData, elementType, elementNullable, customCollectionCls)
}