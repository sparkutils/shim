package org.apache.spark.sql.shim

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.types.{AbstractDataType, DataType}

object StaticInvoke4 {
/* on 3.x, 4.0 adds more
  def apply(
                           staticObject: Class[_],
                           dataType: DataType,
                           functionName: String,
                           arguments: Seq[Expression] = Nil,
                           inputTypes: Seq[AbstractDataType] = Nil,
                           propagateNull: Boolean = true,
                           returnNullable: Boolean = true,
                           isDeterministic: Boolean = true): StaticInvoke
*/
  def apply(
             staticObject: Class[_],
             dataType: DataType,
             functionName: String,
             arguments: Seq[Expression],
             inputTypes: Seq[AbstractDataType] = Nil,
             propagateNull: Boolean = true,
             returnNullable: Boolean = true,
             isDeterministic: Boolean = true
             ) = StaticInvoke(staticObject, dataType, functionName, arguments)

  def unapply(exp: Expression): Option[(Class[_], DataType, String, Seq[Expression])] =
    exp match {
      case StaticInvoke(staticObject,
        dataType,
        functionName,
        arguments,
        propagateNull,
        returnNullable) => Some((staticObject, dataType, functionName, arguments))
      case _ => None
    }
}

object StaticInvoke5 {

  def apply(
             staticObject: Class[_],
             dataType: DataType,
             functionName: String,
             arguments: Seq[Expression],
             inputTypes: Seq[AbstractDataType],
             propagateNull: Boolean = true,
             returnNullable: Boolean = true,
             isDeterministic: Boolean = true
           ) = StaticInvoke(staticObject, dataType, functionName, arguments)

  def unapply(exp: Expression): Option[(Class[_], DataType, String, Seq[Expression], Seq[AbstractDataType])] =
    exp match {
      case StaticInvoke(staticObject,
      dataType,
      functionName,
      arguments,
      propagateNull,
      returnNullable) => Some((staticObject, dataType, functionName, arguments, Seq()))
      case _ => None
    }
}

object StaticInvoke6 {

  def apply(
             staticObject: Class[_],
             dataType: DataType,
             functionName: String,
             arguments: Seq[Expression],
             inputTypes: Seq[AbstractDataType],
             propagateNull: Boolean,
             returnNullable: Boolean = true,
             isDeterministic: Boolean = true
           ) = StaticInvoke(staticObject, dataType, functionName, arguments, propagateNull)

  def unapply(exp: Expression): Option[(Class[_], DataType, String, Seq[Expression], Seq[AbstractDataType], Boolean)] =
    exp match {
      case StaticInvoke(staticObject,
      dataType,
      functionName,
      arguments,
      propagateNull,
      returnNullable) => Some((staticObject, dataType, functionName, arguments, Seq(), propagateNull))
      case _ => None
    }
}

object StaticInvoke7 {

  def apply(
             staticObject: Class[_],
             dataType: DataType,
             functionName: String,
             arguments: Seq[Expression],
             inputTypes: Seq[AbstractDataType],
             propagateNull: Boolean,
             returnNullable: Boolean,
             isDeterministic: Boolean = true
           ) = StaticInvoke(staticObject, dataType, functionName, arguments, propagateNull, returnNullable)

  def unapply(exp: Expression): Option[(Class[_], DataType, String, Seq[Expression], Seq[AbstractDataType], Boolean, Boolean)] =
    exp match {
      case StaticInvoke(staticObject,
      dataType,
      functionName,
      arguments,
      propagateNull,
      returnNullable) => Some((staticObject, dataType, functionName, arguments, Seq(), propagateNull, returnNullable))
      case _ => None
    }
}

object StaticInvoke8 {

  def apply(
             staticObject: Class[_],
             dataType: DataType,
             functionName: String,
             arguments: Seq[Expression],
             inputTypes: Seq[AbstractDataType],
             propagateNull: Boolean,
             returnNullable: Boolean,
             isDeterministic: Boolean
           ) = StaticInvoke(staticObject, dataType, functionName, arguments, propagateNull, returnNullable)

  def unapply(exp: Expression): Option[(Class[_], DataType, String, Seq[Expression], Seq[AbstractDataType], Boolean, Boolean, Boolean)] =
    exp match {
      case StaticInvoke(staticObject,
      dataType,
      functionName,
      arguments,
      propagateNull,
      returnNullable) => Some((staticObject, dataType, functionName, arguments, Seq(), propagateNull, returnNullable, false))
      case _ => None
    }

}
