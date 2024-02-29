package com.sparkutils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.CatalystTypeConverters.convertToCatalyst
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, If, IsNull, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types.DataType

import scala.collection.Map

/**
 * A collection of functions with possibly varying behaviour across Spark versions.  Should actual implementations fracture they will be implemented as part of ShimUtils but the interface will remain to proxy the calls.
 */
package object shim {

  /**
   * Registers a session only plan via experimental methods when isPresentFilter is not true
   * @param logicalPlan
   * @param isPresentFilter a filter that should return true when the plan is identical and it should not be added
   * @return true if the plan has been added
   */
  def registerSessionPlan(logicalPlan: Rule[LogicalPlan])(isPresentFilter: Rule[LogicalPlan] => Boolean): Boolean = {
    val methods = SparkSession.active.sessionState.experimentalMethods
    if (methods.extraOptimizations.forall(!isPresentFilter(_))) {
      methods.extraOptimizations = methods.extraOptimizations :+ logicalPlan
      true
    } else
      false
  }


  /**
   *  work around 2.13 issue / possibly 4 the encoder generates Seq -> ArraySeq$ofRef for a DF with a Seq in it.
   *  CatalystTypeConverters looks for immutable.Seq not collection.Seq, which is different in 2.13 so we need to check for it
   *  RowEncoder is responsible for generating that and wrapped array disappears in 2.13
   */
  def toCatalyst(any: Any): Any = any match {
    case a: scala.collection.mutable.ArraySeq[_] =>
      new GenericArrayData(a.toSeq.map(toCatalyst _))
    case seq: Seq[Any] => new GenericArrayData(seq.map(toCatalyst _).toArray)
    case r: org.apache.spark.sql.Row => InternalRow(r.toSeq.map(toCatalyst _): _*)
    case arr: Array[_] => new GenericArrayData(arr.map(toCatalyst _))
    case map: Map[_, _] =>
      ArrayBasedMapData(
        map,
        (key: Any) => toCatalyst(key),
        (value: Any) => toCatalyst(value))

    case other => convertToCatalyst(other)
  }

  // below are for Framless support
  def deriveUnitLiteral: Expression = Literal.fromObject(())

  /**
   * If the path is null then uses a null literal with dataType, if it's not null it uses the nonNullExpr
   * @param dataType
   * @param path
   * @param nonNullExpr
   * @return
   */
  def ifIsNull(dataType: DataType, path: Expression, nonNullExpr: Expression): Expression = {
    val nullExpr = Literal.create(null, dataType)

    If(IsNull(path), nullExpr, nonNullExpr)
  }
}

