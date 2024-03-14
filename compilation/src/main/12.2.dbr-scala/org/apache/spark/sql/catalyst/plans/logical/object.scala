package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.analysis.UnresolvedDeserializer
import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SortOrder}

object MapGroups {
  def apply[K : Encoder, T : Encoder, U : Encoder](
                                                    func: (K, Iterator[T]) => TraversableOnce[U],
                                                    groupingAttributes: Seq[Attribute],
                                                    dataAttributes: Seq[Attribute],
                                                    dataOrder: Seq[SortOrder],
                                                    child: LogicalPlan): LogicalPlan = {
    val mapped = new MapGroups(
      func.asInstanceOf[(Any, Iterator[Any]) => TraversableOnce[Any]],
      UnresolvedDeserializer(encoderFor[K].deserializer, groupingAttributes),
      UnresolvedDeserializer(encoderFor[T].deserializer, dataAttributes),
      groupingAttributes,
      dataAttributes,
      dataOrder,
      CatalystSerde.generateObjAttr[U],
      child)
    CatalystSerde.serialize[U](mapped)
  }
}

case class MapGroups(
                      func: (Any, Iterator[Any]) => TraversableOnce[Any],
                      keyDeserializer: Expression,
                      valueDeserializer: Expression,
                      groupingAttributes: Seq[Attribute],
                      dataAttributes: Seq[Attribute],
                      dataOrder: Seq[SortOrder],
                      outputObjAttr: Attribute,
                      child: LogicalPlan) extends UnaryNode with ObjectProducer {
  override protected def withNewChildInternal(newChild: LogicalPlan): MapGroups =
    copy(child = newChild)
}