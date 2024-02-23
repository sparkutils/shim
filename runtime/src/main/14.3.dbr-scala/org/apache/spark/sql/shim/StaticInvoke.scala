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

  def unapply(exp: StaticInvoke): Option[(Class[_], DataType, String, Seq[Expression])] =
    exp match {
      case StaticInvoke(staticObject,
        dataType,
        functionName,
        arguments,
        inputTypes,
        propagateNull,
        returnNullable,
        isDeterministic, _) => Some((staticObject, dataType, functionName, arguments))
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
           ) = StaticInvoke(staticObject, dataType, functionName, arguments, inputTypes)

  def unapply(exp: StaticInvoke): Option[(Class[_], DataType, String, Seq[Expression], Seq[AbstractDataType])] =
    exp match {
      case StaticInvoke(staticObject,
      dataType,
      functionName,
      arguments,
      inputTypes,
      propagateNull,
      returnNullable,
      isDeterministic, _) => Some((staticObject, dataType, functionName, arguments, inputTypes))
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
           ) = StaticInvoke(staticObject, dataType, functionName, arguments, inputTypes, propagateNull)

  def unapply(exp: StaticInvoke): Option[(Class[_], DataType, String, Seq[Expression], Seq[AbstractDataType], Boolean)] =
    exp match {
      case StaticInvoke(staticObject,
      dataType,
      functionName,
      arguments,
      inputTypes,
      propagateNull,
      returnNullable,
      isDeterministic, _) => Some((staticObject, dataType, functionName, arguments, inputTypes, propagateNull))
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
           ) = StaticInvoke(staticObject, dataType, functionName, arguments, inputTypes, propagateNull, returnNullable)

  def unapply(exp: StaticInvoke): Option[(Class[_], DataType, String, Seq[Expression], Seq[AbstractDataType], Boolean, Boolean)] =
    exp match {
      case StaticInvoke(staticObject,
      dataType,
      functionName,
      arguments,
      inputTypes,
      propagateNull,
      returnNullable,
      isDeterministic, _) => Some((staticObject, dataType, functionName, arguments, inputTypes, propagateNull, returnNullable))
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
           ) = StaticInvoke(staticObject, dataType, functionName, arguments, inputTypes, propagateNull, returnNullable, isDeterministic)

  def unapply(exp: StaticInvoke): Option[(Class[_], DataType, String, Seq[Expression], Seq[AbstractDataType], Boolean, Boolean, Boolean)] =
    exp match {
      case StaticInvoke(staticObject,
      dataType,
      functionName,
      arguments,
      inputTypes,
      propagateNull,
      returnNullable,
      isDeterministic, _) => Some((staticObject, dataType, functionName, arguments, inputTypes, propagateNull, returnNullable, isDeterministic))
      case _ => None
    }

}
