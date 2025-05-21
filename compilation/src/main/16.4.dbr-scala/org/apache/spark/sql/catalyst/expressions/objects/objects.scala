package org.apache.spark.sql.catalyst.expressions.objects

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.EncoderUtils
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction
import org.apache.spark.sql.types.{AbstractDataType, DataType}
import org.apache.spark.util.Utils


/**
 * Invokes a static function, returning the result.  By default, any of the arguments being null
 * will result in returning null instead of calling the function.
 *
 * @param staticObject The target of the static call.  This can either be the object itself
 *                     (methods defined on scala objects), or the class object
 *                     (static methods defined in java).
 * @param dataType The expected return type of the function call
 * @param functionName The name of the method to call.
 * @param arguments An optional list of expressions to pass as arguments to the function.
 * @param inputTypes A list of data types specifying the input types for the method to be invoked.
 *                   If enabled, it must have the same length as [[arguments]]. In case an input
 *                   type differs from the actual argument type, Spark will try to perform
 *                   type coercion and insert cast whenever necessary before invoking the method.
 *                   The above is disabled if this is empty.
 * @param propagateNull When true, and any of the arguments is null, null will be returned instead
 *                      of calling the function. Also note: when this is false but any of the
 *                      arguments is of primitive type and is null, null also will be returned
 *                      without invoking the function.
 * @param returnNullable When false, indicating the invoked method will always return
 *                       non-null value.
 * @param isDeterministic Whether the method invocation is deterministic or not. If false, Spark
 *                        will not apply certain optimizations such as constant folding.
 */
case class StaticInvoke(
                         staticObject: Class[_],
                         dataType: DataType,
                         functionName: String,
                         arguments: Seq[Expression] = Nil,
                         inputTypes: Seq[AbstractDataType] = Nil,
                         propagateNull: Boolean = true,
                         returnNullable: Boolean = true,
                         isDeterministic: Boolean = true,
                         scalarFunction: Option[ScalarFunction[_]] = None) extends InvokeLike {

  val objectName = staticObject.getName.stripSuffix("$")
  val cls = if (staticObject.getName == objectName) {
    staticObject
  } else {
    Utils.classForName(objectName)
  }

  override def nullable: Boolean = needNullCheck || returnNullable
  override def children: Seq[Expression] = arguments
  override lazy val deterministic: Boolean = isDeterministic && arguments.forall(_.deterministic)

  lazy val argClasses = EncoderUtils.expressionJavaClasses(arguments)
  @transient lazy val method = findMethod(cls, functionName, argClasses)

  override def eval(input: InternalRow): Any = {
    invoke(null, method, input)
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val javaType = CodeGenerator.javaType(dataType)

    val (argCode, argString, resultIsNull) = prepareArguments(ctx)

    val callFunc = s"$objectName.$functionName($argString)"

    val prepareIsNull = if (nullable) {
      s"boolean ${ev.isNull} = $resultIsNull;"
    } else {
      ev.isNull = FalseLiteral
      ""
    }

    val evaluate = if (returnNullable && !method.getReturnType.isPrimitive) {
      if (CodeGenerator.defaultValue(dataType) == "null") {
        s"""
          ${ev.value} = $callFunc;
          ${ev.isNull} = ${ev.value} == null;
        """
      } else {
        val boxedResult = ctx.freshName("boxedResult")
        s"""
          ${CodeGenerator.boxedType(dataType)} $boxedResult = $callFunc;
          ${ev.isNull} = $boxedResult == null;
          if (!${ev.isNull}) {
            ${ev.value} = $boxedResult;
          }
        """
      }
    } else {
      s"${ev.value} = $callFunc;"
    }

    val code = code"""
      $argCode
      $prepareIsNull
      $javaType ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
      if (!$resultIsNull) {
        $evaluate
      }
     """
    ev.copy(code = code)
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(arguments = newChildren)
}