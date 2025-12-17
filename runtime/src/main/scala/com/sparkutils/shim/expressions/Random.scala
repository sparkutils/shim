package com.sparkutils.shim.expressions

import org.apache.spark.sql.catalyst.expressions.ExpressionWithRandomSeed

trait ExpressionWithRandomSeedLike extends ExpressionWithRandomSeed {
  def currentSeed: Long = 0L
  def withShiftedSeed(shift: Long): org.apache.spark.sql.catalyst.expressions.Expression =
    withNewSeed(currentSeed + shift)
}
