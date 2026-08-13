package org.apache.spark.sql.shim

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.types.{AbstractDataType, DataType}

/*
Invoke change caused breakage in 3.2.0 to 3.2.1

3.5 below
    targetObject: Expression,
    functionName: String,
    dataType: DataType,
    arguments: Seq[Expression] = Nil,
    methodInputTypes: Seq[AbstractDataType] = Nil,
    propagateNull: Boolean = true,
    returnNullable : Boolean = true,
    isDeterministic: Boolean = true)
 */

object Invoke5 {

  def apply(targetObject: Expression,
            functionName: String,
            dataType: DataType,
            arguments: Seq[Expression] = Nil,
            methodInputTypes: Seq[AbstractDataType] = Nil): Expression =
    Invoke(targetObject, functionName, dataType, arguments, methodInputTypes)

  def unapply(cast: Invoke): Option[(Expression,String,DataType,Seq[Expression],Seq[AbstractDataType])] =
    cast match {
      case Invoke(targetObject, functionName, dataType, arguments, methodInputTypes, propagateNull, returnNullable, isDeterministic) =>
        Some((targetObject, functionName, dataType, arguments, methodInputTypes))
      case _ => None
    }
}

object Invoke6 {

  def apply(targetObject: Expression,
            functionName: String,
            dataType: DataType,
            arguments: Seq[Expression] = Nil,
            methodInputTypes: Seq[AbstractDataType] = Nil,
            propagateNull: Boolean = true): Expression =
    Invoke(targetObject, functionName, dataType, arguments, methodInputTypes, propagateNull)

  def unapply(cast: Invoke): Option[(Expression,String,DataType,Seq[Expression],Seq[AbstractDataType],Boolean)] =
    cast match {
      case Invoke(targetObject, functionName, dataType, arguments, methodInputTypes, propagateNull, returnNullable, isDeterministic) =>
        Some((targetObject, functionName, dataType, arguments, methodInputTypes, propagateNull))
      case _ => None
    }
}

object Invoke7 {

  def apply(targetObject: Expression,
            functionName: String,
            dataType: DataType,
            arguments: Seq[Expression] = Nil,
            methodInputTypes: Seq[AbstractDataType] = Nil,
            propagateNull: Boolean = true,
            returnNullable : Boolean = true): Expression =
    Invoke(targetObject, functionName, dataType, arguments, methodInputTypes, propagateNull, returnNullable)

  def unapply(cast: Invoke): Option[(Expression,String,DataType,Seq[Expression],Seq[AbstractDataType],Boolean,Boolean)] =
    cast match {
      case Invoke(targetObject, functionName, dataType, arguments, methodInputTypes, propagateNull, returnNullable, isDeterministic) =>
        Some((targetObject, functionName, dataType, arguments, methodInputTypes, propagateNull, returnNullable))
      case _ => None
    }
}

object Invoke8 {

  def apply(targetObject: Expression,
            functionName: String,
            dataType: DataType,
            arguments: Seq[Expression] = Nil,
            methodInputTypes: Seq[AbstractDataType] = Nil,
            propagateNull: Boolean = true,
            returnNullable : Boolean = true,
            isDeterministic: Boolean = true): Expression =
    Invoke(targetObject, functionName, dataType, arguments, methodInputTypes, propagateNull, returnNullable, isDeterministic)

  def unapply(cast: Invoke): Option[(Expression,String,DataType,Seq[Expression],Seq[AbstractDataType],Boolean,Boolean,Boolean)] =
    cast match {
      case Invoke(targetObject, functionName, dataType, arguments, methodInputTypes, propagateNull, returnNullable, isDeterministic) =>
        Some((targetObject, functionName, dataType, arguments, methodInputTypes, propagateNull, returnNullable, isDeterministic))
      case _ => None
    }
}
