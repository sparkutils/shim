package org.apache.spark.sql.shim

import com.sparkutils.shim.ShowParams
import org.apache.spark.sql.ShimUtils.{column, expression}
import org.apache.spark.sql.catalyst.expressions.{Expression, LambdaFunction, NamedExpression, UnresolvedNamedLambdaVariable}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{Column, DataFrame, Dataset, ShimUtils, SparkSession}

import java.util.concurrent.atomic.AtomicInteger

object utils {

  def toString(dataFrame: DataFrame, showParams: ShowParams = ShowParams()) =
    dataFrame.showString(showParams.numRows, showParams.truncate, showParams.vertical)

  /**
   * 4 preview2 moves named to ExpressionUtils, as such this forwards to ShimUtils.toNamed
   * @param expression
   * @return
   */
  @deprecated(since = "0.0.1-RC5",message = "Use ShimUtils.toNamed directly")
  def named(col: Column): NamedExpression = ShimUtils.toNamed(col)

  // taken from functions, where they are private
  def createLambda(f: Column => Column) = {
    val x = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariableT.freshVarName("x")))
    val function = expression(f(column(x)))
    LambdaFunction(function, Seq(x))
  }

  def createLambda(f: (Column, Column) => Column) = {
    val x = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariableT.freshVarName("x")))
    val y = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariableT.freshVarName("y")))
    val function = expression(f(column(x), column(y)))
    LambdaFunction(function, Seq(x, y))
  }

  def createLambda(f: (Column, Column, Column) => Column) = {
    val x = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariableT.freshVarName("x")))
    val y = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariableT.freshVarName("y")))
    val z = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariableT.freshVarName("z")))
    val function = expression(f(column(x), column(y), column(z)))
    LambdaFunction(function, Seq(x, y, z))
  }

  // below support moving FramelessInternals to frameless
  def logicalPlan(ds: Dataset[_]): LogicalPlan = ds.logicalPlan

  def ofRows(sparkSession: SparkSession, logicalPlan: LogicalPlan): DataFrame =
    Dataset.ofRows(sparkSession, logicalPlan)

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

