package org.apache.spark.sql

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types._

/**
 * Set of utilities to reach in to private functions which are agnostic safe but provide mostly compatible non-agnostic interfaces
 */
object ShimAgnosticUtils {

  def toSQLType(dataType: DataType): String = ShimUtils.toSQLType(dataType)

  def tableOrViewNotFound(e: Exception): Option[Either[Exception, Set[String]]] = ShimUtils.tableOrViewNotFound(e)

  def rowEncoder(structType: StructType): ExpressionEncoder[Row] = ShimUtils.rowEncoder(structType)

  // Below are added for Frameless RowEncoder, TypedEncoder and TypedExpressionEncoder support
  def targetStructType(dataType: DataType, nullable: Boolean): StructType =
    dataType match {
      case x: StructType =>
        if (nullable) StructType(x.fields.map(_.copy(nullable = true)))
        else x

      case dt => new StructType().add("value", dt, nullable = nullable)
    }

  def analysisException(ds: Dataset[_], colNames: Seq[String]): AnalysisException = ShimUtils.analysisException(ds, colNames)

  /**
   * Wrapper around call_function introduced in 3.5.0.  Spark 4 shims and above use internal.UnresolvedFunction
   * and below uses analysis.UnresolvedFunction
   * @param funcName
   * @param cols
   * @return
   */
  @scala.annotation.varargs
  def callFunction(funcName: String, cols: Column*): Column = ShimUtils.callFunction(funcName, cols : _*)

  /**
   * Returns true if the sparkSession is in classic mode, false for connect.  On pre 4.0.0 versions this
   * always returns true.  NB 4 and above uses reflection
   * @param sparkSession
   * @return
   */
  def isClassic(sparkSession: SparkSession): Boolean = ShimUtils.isClassic(sparkSession)

  def isUsable(sparkSession: SparkSession): Boolean = ShimUtils.isUsable(sparkSession)

}
