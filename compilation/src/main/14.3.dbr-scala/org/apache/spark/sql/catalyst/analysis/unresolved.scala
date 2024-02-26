package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.types.DataType

/**
 * Represents an unresolved function that is being invoked. The analyzer will resolve the function
 * arguments first, then look up the function by name and arguments, and return an expression that
 * can be evaluated to get the result of this function invocation.
 */
case class UnresolvedFunction(
                               nameParts: Seq[String],
                               arguments: Seq[Expression],
                               isDistinct: Boolean,
                               filter: Option[Expression] = None,
                               ignoreNulls: Boolean = false,
                               orderingWithinGroup: Seq[SortOrder] = Seq.empty)
  extends Expression with Unevaluable {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override def children: Seq[Expression] = arguments ++ filter.toSeq ++ orderingWithinGroup

  override def dataType: DataType = throw new UnresolvedException("dataType")
  override def nullable: Boolean = throw new UnresolvedException("nullable")
  override lazy val resolved = false
  final override val nodePatterns: Seq[TreePattern] = Seq(UNRESOLVED_FUNCTION)

  override def prettyName: String = nameParts.quoted
  override def toString: String = {
    val distinct = if (isDistinct) "distinct " else ""
    s"'${nameParts.quoted}($distinct${children.mkString(", ")})"
  }

  override protected def withNewChildrenInternal(
                                                  newChildren: IndexedSeq[Expression]): UnresolvedFunction = {
    if (filter.isDefined) {
      if (orderingWithinGroup.isEmpty) {
        copy(arguments = newChildren.dropRight(1), filter = Some(newChildren.last))
      } else {
        val nonArgs = newChildren.takeRight(orderingWithinGroup.length + 1)
        val newSortOrders = nonArgs.tail.asInstanceOf[Seq[SortOrder]]
        val newFilter = Some(nonArgs.head)
        val newArgs = newChildren.dropRight(orderingWithinGroup.length + 1)
        copy(arguments = newArgs, filter = newFilter, orderingWithinGroup = newSortOrders)
      }
    } else if (orderingWithinGroup.isEmpty) {
      copy(arguments = newChildren)
    } else {
      val newSortOrders =
        newChildren.takeRight(orderingWithinGroup.length).asInstanceOf[Seq[SortOrder]]
      val newArgs = newChildren.dropRight(orderingWithinGroup.length)
      copy(arguments = newArgs, orderingWithinGroup = newSortOrders)
    }
  }
}

object UnresolvedFunction {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  def apply(
             name: FunctionIdentifier,
             arguments: Seq[Expression],
             isDistinct: Boolean): UnresolvedFunction = {
    UnresolvedFunction(name.asMultipart, arguments, isDistinct)
  }

  def apply(name: String, arguments: Seq[Expression], isDistinct: Boolean): UnresolvedFunction = {
    UnresolvedFunction(Seq(name), arguments, isDistinct)
  }
}
