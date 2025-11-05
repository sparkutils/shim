package com.sparkutils.shim

import org.apache.spark.sql.{Column, internal}

object LambdaFunctions {

  // taken from functions, where they are private
  def createLambda(f: Column => Column): Column = {
    val x = internal.UnresolvedNamedLambdaVariable("x")
    val function = f(new Column(x)).node
    new Column(internal.LambdaFunction(function, Seq(x)))
  }

  def createLambda(f: (Column, Column) => Column): Column = {
    val x = internal.UnresolvedNamedLambdaVariable("x")
    val y = internal.UnresolvedNamedLambdaVariable("y")
    val function = f(new Column(x), new Column(y)).node
    new Column(internal.LambdaFunction(function, Seq(x, y)))
  }

  def createLambda(f: (Column, Column, Column) => Column): Column = {
    val x = internal.UnresolvedNamedLambdaVariable("x")
    val y = internal.UnresolvedNamedLambdaVariable("y")
    val z = internal.UnresolvedNamedLambdaVariable("z")
    val function = f(new Column(x), new Column(y), new Column(z)).node
    new Column(internal.LambdaFunction(function, Seq(x, y, z)))
  }

}
