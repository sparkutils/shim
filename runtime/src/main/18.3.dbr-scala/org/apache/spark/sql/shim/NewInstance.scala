package org.apache.spark.sql.shim

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.NewInstance
import org.apache.spark.sql.types.{AbstractDataType, DataType}

/*

from 2.4

NewInstance(
    cls: Class[_],
    arguments: Seq[Expression],
    propagateNull: Boolean,
    dataType: DataType,
    outerPointer: Option[() => AnyRef])

from 3.5

  cls: Class[_],
    arguments: Seq[Expression],
    inputTypes: Seq[AbstractDataType],
    propagateNull: Boolean,
    dataType: DataType,
    outerPointer: Option[() => AnyRef]
 */


object NewInstance4 {
  /*
  The 4 field interface is actually stable for 2.4 -> 3.5, used by Frameless, adding 5th default as it covers most usage
   */
  def apply(
             cls: Class[_],
             arguments: Seq[Expression],
             dataType: DataType,
             propagateNull: Boolean = true
           ): NewInstance =
    new NewInstance(cls, arguments, inputTypes = Nil, propagateNull, dataType, None)

  def unapply(cast: NewInstance): Option[(Class[_],Seq[Expression],DataType,Boolean)] =
    cast match {
      case NewInstance(cls,
      arguments,
      inputTypes,
      propagateNull,
      dataType,
      outerPointer) =>
        Some((cls,
          arguments,
          dataType,
          propagateNull))
      case _ => None
    }
}

object NewInstance5 {
  /*
  The 4 field interface is actually stable for 2.4 -> 3.5, used by Frameless, adding 5th default as it covers most usage
   */
  def apply(
             cls: Class[_],
             arguments: Seq[Expression],
             dataType: DataType,
             propagateNull: Boolean = true,
             outerPointer: Option[() => AnyRef] = None
            ): NewInstance =
    new NewInstance(cls, arguments, inputTypes = Nil, propagateNull, dataType, outerPointer)
  def unapply(cast: NewInstance): Option[(Class[_],Seq[Expression],DataType,Boolean,Option[() => AnyRef])] =
    cast match {
      case NewInstance(cls,
      arguments,
      inputTypes,
      propagateNull,
      dataType,
      outerPointer) =>
        Some((cls,
          arguments,
          dataType,
          propagateNull,outerPointer))
      case _ => None
    }
}

object NewInstance6 {
  def apply(cls: Class[_],
            arguments: Seq[Expression],
            inputTypes: Seq[AbstractDataType],
            propagateNull: Boolean,
            dataType: DataType,
            outerPointer: Option[() => AnyRef]): Expression =
    NewInstance(cls, arguments, inputTypes, propagateNull, dataType, outerPointer)

  def unapply(cast: NewInstance): Option[(Class[_],Seq[Expression],Seq[AbstractDataType],Boolean,DataType,Option[() => AnyRef])] =
    cast match {
      case NewInstance(cls,
      arguments,
      inputTypes,
      propagateNull,
      dataType,
      outerPointer) =>
        Some((cls,
          arguments,
          inputTypes,
          propagateNull,
          dataType,
          outerPointer))
      case _ => None
    }
}