package org.apache.spark.sql

import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, RowEncoder}
import org.apache.spark.sql.errors.DataTypeErrors.{toSQLType => stoSQLType}
import org.apache.spark.sql.types._

/**
 * Set of utilities to reach in to private functions which are agnostic safe but provide mostly compatible non-agnostic interfaces
 */
object ShimAgnosticUtils {

  def toSQLType(dataType: DataType): String = stoSQLType(dataType)

  def tableOrViewNotFound(e: Exception): Option[Either[Exception, Set[String]]] = ShimUtils.tableOrViewNotFound(e)

  def rowEncoder(structType: StructType): AgnosticEncoder[Row] = RowEncoder.encoderFor(structType)

  // Below are added for Frameless RowEncoder, TypedEncoder and TypedExpressionEncoder support
  def targetStructType(dataType: DataType, nullable: Boolean): StructType =
    dataType match {
      case x: StructType =>
        if (nullable) StructType(x.fields.map(_.copy(nullable = true)))
        else x

      case dt => new StructType().add("value", dt, nullable = nullable)
    }

  def analysisException(ds: Dataset[_], colNames: Seq[String]): AnalysisException =
    new AnalysisException( s"""Cannot resolve column name "$colNames" among (${ds.schema.fieldNames.mkString(", ")})""", messageParameters = Map.empty )

  /**
   * Wrapper around call_function introduced in 3.5.0.  Spark 4 shims and above use internal.UnresolvedFunction
   * and below uses analysis.UnresolvedFunction
   * @param funcName
   * @param cols
   * @return
   */
  @scala.annotation.varargs
  def callFunction(funcName: String, cols: Column*): Column =
    Column(internal.UnresolvedFunction(funcName, cols.map(_.node), isUserDefinedFunction = true))

  /**
   * Returns true if the sparkSession is in classic mode, false for connect.  On pre 4.0.0 versions this
   * always returns true.  NB 4 and above uses reflection
   * @param sparkSession
   * @return
   */
  def isClassic(sparkSession: SparkSession): Boolean =
    try {
      val cc = classOf[SparkSession].getClassLoader.loadClass("org.apache.spark.sql.classic.SparkSession")
      cc.isInstance(sparkSession)
    } catch {
      case _: ClassNotFoundException => false
    }

  def isUsable(sparkSession: SparkSession): Boolean =
    sparkSession.isUsable

}
