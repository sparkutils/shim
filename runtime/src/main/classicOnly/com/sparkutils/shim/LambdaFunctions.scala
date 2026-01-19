package com.sparkutils.shim

import org.apache.spark.sql.Column
import org.apache.spark.sql.ShimUtils.{column, expression}
import org.apache.spark.sql.catalyst.expressions.{LambdaFunction, UnresolvedNamedLambdaVariable}

object LambdaFunctions {

  // taken from functions, where they are private
  def createLambda(f: Column => Column): Column = {
    val x = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariable.freshVarName("x")))
    val function = expression(f(column(x)))
    column(LambdaFunction(function, Seq(x)))
  }

  def createLambda(f: (Column, Column) => Column): Column = {
    val x = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariable.freshVarName("x")))
    val y = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariable.freshVarName("y")))
    val function = expression(f(column(x), column(y)))
    column(LambdaFunction(function, Seq(x, y)))
  }

  def createLambda(f: (Column, Column, Column) => Column): Column = {
    val x = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariable.freshVarName("x")))
    val y = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariable.freshVarName("y")))
    val z = UnresolvedNamedLambdaVariable(Seq(UnresolvedNamedLambdaVariable.freshVarName("z")))
    val function = expression(f(column(x), column(y), column(z)))
    column(LambdaFunction(function, Seq(x, y, z)))
  }

}
