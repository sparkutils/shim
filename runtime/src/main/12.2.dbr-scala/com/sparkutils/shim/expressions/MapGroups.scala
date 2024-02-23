package com.sparkutils.shim.expressions

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, MapGroups => SMapGroups}

object MapGroups4 {
  def apply[K: Encoder, T: Encoder, U: Encoder](
                                                 func: (K, Iterator[T]) => TraversableOnce[U],
                                                 groupingAttributes: Seq[Attribute],
                                                 dataAttributes: Seq[Attribute],
                                                 child: LogicalPlan
                                               ): LogicalPlan = SMapGroups(func, groupingAttributes, dataAttributes, child)
}

object MapGroups5 {
  def apply[K: Encoder, T: Encoder, U: Encoder](
                                                 func: (K, Iterator[T]) => TraversableOnce[U],
                                                 groupingAttributes: Seq[Attribute],
                                                 dataAttributes: Seq[Attribute],
                                                 dataOrder: Seq[SortOrder],
                                                 child: LogicalPlan
                                               ): LogicalPlan =
    SMapGroups(
      func,
      groupingAttributes,
      dataAttributes,
      child
    )
}
