package com.sparkutils.shim.expressions

import org.apache.spark.sql.catalyst.expressions.{Expression, HigherOrderFunction, Nondeterministic, Unevaluable}
// DBR 15.4 added nonVolatile
trait NondeterministicLike extends Nondeterministic {

}
trait StatefulLike extends Nondeterministic {
  /**
   * Return a fresh uninitialized copy of the stateful expression.
   */
  def freshCopy(): StatefulLike
  override def stateful: Boolean = true

}

trait HigherOrderFunctionLike extends HigherOrderFunction {}

/**
 * 2.4 and 3.0 version doesn't have foldable as false so the optimiser tries to fold, we need Unevaluable for 14.4
 */
trait FoldableUnevaluable extends Unevaluable {
}

// dropped in 4.0
trait NullIntolerant extends org.apache.spark.sql.catalyst.expressions.NullIntolerant
