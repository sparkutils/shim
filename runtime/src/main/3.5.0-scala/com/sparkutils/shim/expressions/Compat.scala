package com.sparkutils.shim.expressions

import org.apache.spark.sql.catalyst.expressions.{HigherOrderFunction, Nondeterministic, Unevaluable}

trait StatefulLike extends Nondeterministic {
  /**
   * Return a fresh uninitialized copy of the stateful expression.
   */
  def freshCopy(): StatefulLike

}

trait HigherOrderFunctionLike extends HigherOrderFunction {}


/**
 * 2.4 till some 3 version doesn't have foldable as false so the optimiser tries to fold, we need Unevaluable for 14.4
 */
trait FoldableUnevaluable extends Unevaluable {
}