package com.sparkutils.shim

import org.apache.spark.sql.Column
import org.apache.spark.sql.ShimUtils.{column, expression}
import org.apache.spark.sql.catalyst.expressions.{LambdaFunction, UnresolvedNamedLambdaVariable}
import org.apache.spark.sql.shim.UnresolvedNamedLambdaVariableT

object LambdaFunctions {

  // taken from functions, where they are private
  def createLambda(f: Column => Column): Column = {
    val x = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariableT.freshVarName("x")))
    val function = expression(f(column(x)))
    column(LambdaFunction(function, Seq(x)))
  }

  def createLambda(f: (Column, Column) => Column): Column = {
    val x = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariableT.freshVarName("x")))
    val y = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariableT.freshVarName("y")))
    val function = expression(f(column(x), column(y)))
    column(LambdaFunction(function, Seq(x, y)))
  }

  def createLambda(f: (Column, Column, Column) => Column): Column = {
    val x = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariableT.freshVarName("x")))
    val y = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariableT.freshVarName("y")))
    val z = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariableT.freshVarName("z")))
    val function = expression(f(column(x), column(y), column(z)))
    column(LambdaFunction(function, Seq(x, y, z)))
  }

}
