package com.sparkutils.shim.expressions

/*
 * Collection of shims on really stable interfaces for logical expressions
 */

import org.apache.spark.sql.catalyst.expressions.{And, Expression, Or, EqualNullSafe, EqualTo, Not, IsNull, IsNotNull}

object And2 {
  def apply(left: Expression, right: Expression): And =
    And(left, right)

  def unapply(and: And): Option[(Expression, Expression)] =
    And.unapply(and)
}

object Or2 {
  def apply(left: Expression, right: Expression): Or =
    Or(left, right)

  def unapply(or: Or): Option[(Expression, Expression)] =
    Or.unapply(or)
}

object EqualNullSafe2 {
  def apply(left: Expression, right: Expression): EqualNullSafe =
    EqualNullSafe(left, right)

  def unapply(equalNullSafe: EqualNullSafe): Option[(Expression, Expression)] =
    EqualNullSafe.unapply(equalNullSafe)
}

object EqualTo2 {
  def apply(left: Expression, right: Expression): EqualTo =
    EqualTo(left, right)

  def unapply(equalTo: EqualTo): Option[(Expression, Expression)] =
    EqualTo.unapply(equalTo)
}

object Not1 {
  def apply(child: Expression): Not =
    Not(child)

  def unapply(not: Not): Option[Expression] =
    Not.unapply(not)
}

object IsNull1 {
  def apply(child: Expression): IsNull =
    IsNull(child)

  def unapply(isNull: IsNull): Option[Expression] =
    IsNull.unapply(isNull)
}

object IsNotNull1 {
  def apply(child: Expression): IsNotNull =
    IsNotNull(child)

  def unapply(isNotNull: IsNotNull): Option[Expression] =
    IsNotNull.unapply(isNotNull)
}