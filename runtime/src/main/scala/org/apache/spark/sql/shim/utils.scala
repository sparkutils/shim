package org.apache.spark.sql.shim

import com.sparkutils.shim.{LambdaFunctions, ShowParams}
import org.apache.spark.sql.ShimUtils.{column, expression}
import org.apache.spark.sql.catalyst.expressions.{LambdaFunction, NamedExpression, UnresolvedNamedLambdaVariable}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{Column, DataFrame, Dataset, ShimUtils, SparkSession}

import java.util.concurrent.atomic.AtomicInteger

object utils {

  def toString(dataFrame: DataFrame, showParams: ShowParams = ShowParams()) =
    ShimUtils.toString(dataFrame, showParams)

  /**
   * 4 preview2 moves named to ExpressionUtils, as such this forwards to ShimUtils.toNamed
   * @param expression
   * @return
   */
  @deprecated(since = "0.0.1-RC5",message = "Use ShimUtils.toNamed directly")
  def named(col: Column): NamedExpression = ShimUtils.toNamed(col)

  def createLambda(f: Column => Column): Column = LambdaFunctions.createLambda(f)

  def createLambda(f: (Column, Column) => Column): Column = LambdaFunctions.createLambda(f)

  def createLambda(f: (Column, Column, Column) => Column): Column = LambdaFunctions.createLambda(f)

  // below support moving FramelessInternals to frameless
  def logicalPlan(ds: Dataset[_]): LogicalPlan = ShimUtils.logicalPlan(ds)

  def ofRows(sparkSession: SparkSession, logicalPlan: LogicalPlan): DataFrame =
    ShimUtils.ofRows(sparkSession, logicalPlan)

}

object mlUtils {

  // because org.apache.spark.ml.linalg.VectorUDT is private[spark]
  val vectorUdt = new org.apache.spark.ml.linalg.VectorUDT

  // because org.apache.spark.ml.linalg.MatrixUDT is private[spark]
  val matrixUdt = new org.apache.spark.ml.linalg.MatrixUDT

}

// TODO remove with 2.4, it's only here until 0.2.0 as 3 introduced freshVarName
object UnresolvedNamedLambdaVariableT {

  // Counter to ensure lambda variable names are unique
  private val nextVarNameId = new AtomicInteger(0)

  def freshVarName(name: String): String = {
    s"${name}_${nextVarNameId.getAndIncrement()}"
  }
}

