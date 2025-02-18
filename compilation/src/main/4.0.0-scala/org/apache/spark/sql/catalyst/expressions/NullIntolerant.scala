package org.apache.spark.sql.catalyst.expressions
trait NullIntolerant extends Expression {
  /**
   * When an expression inherits this, meaning the expression is null intolerant (i.e. any null
   * input will result in null output). We will use this information during constructing IsNotNull
   * constraints.
   */
  override def nullIntolerant: Boolean = true
}