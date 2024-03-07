package org.apache.spark.sql.shim

import com.sparkutils.shim.ShowParams
import org.apache.spark.sql.catalyst.expressions.{LambdaFunction, NamedExpression, UnresolvedNamedLambdaVariable}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

import java.util.concurrent.atomic.AtomicInteger

object utils {

  def toString(dataFrame: DataFrame, showParams: ShowParams = ShowParams()) =
    dataFrame.showString(showParams.numRows, showParams.truncate, showParams.vertical)

  def named(col: Column): NamedExpression = col.named

  // taken from functions, where they are private
  def createLambda(f: Column => Column) = {
    val x = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariableT.freshVarName("x")))
    val function = f(Column(x)).expr
    LambdaFunction(function, Seq(x))
  }

  def createLambda(f: (Column, Column) => Column) = {
    val x = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariableT.freshVarName("x")))
    val y = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariableT.freshVarName("y")))
    val function = f(Column(x), Column(y)).expr
    LambdaFunction(function, Seq(x, y))
  }

  def createLambda(f: (Column, Column, Column) => Column) = {
    val x = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariableT.freshVarName("x")))
    val y = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariableT.freshVarName("y")))
    val z = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariableT.freshVarName("z")))
    val function = f(Column(x), Column(y), Column(z)).expr
    LambdaFunction(function, Seq(x, y, z))
  }

  def logicalPlan(ds: Dataset[_]): LogicalPlan = ds.logicalPlan

  def ofRows(sparkSession: SparkSession, logicalPlan: LogicalPlan): DataFrame =
    Dataset.ofRows(sparkSession, logicalPlan)

}

// TODO remove with 2.4, it's only here until 0.2.0 as 3 introduced freshVarName
object UnresolvedNamedLambdaVariableT {

  // Counter to ensure lambda variable names are unique
  private val nextVarNameId = new AtomicInteger(0)

  def freshVarName(name: String): String = {
    s"${name}_${nextVarNameId.getAndIncrement()}"
  }
}

