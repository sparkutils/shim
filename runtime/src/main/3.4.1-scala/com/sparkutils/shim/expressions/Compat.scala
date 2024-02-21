package com.sparkutils.shim.expressions

import org.apache.spark.sql.catalyst.expressions.{HigherOrderFunction, Nondeterministic}

trait StatefulLike extends Nondeterministic {
  /**
   * Return a fresh uninitialized copy of the stateful expression.
   */
  def freshCopy(): StatefulLike

}

trait HigherOrderFunctionLike extends HigherOrderFunction {}