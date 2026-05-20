package com.sparkutils.shim.codegen

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator.{JAVA_BOOLEAN, MAX_JVM_METHOD_PARAMS_LENGTH, calculateParamLengthFromExprValues, getLocalInputVariableValues, isValidParamLength, javaType}
import org.apache.spark.sql.catalyst.expressions.{EquivalentExpressions, Expression, ExpressionEquals}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral, JavaCode, ShimExprUtils, SubExprCodes, SubExprEliminationState, TrueLiteral}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

import scala.collection.mutable

object SubExprCodeGen extends Logging {
  /**
   * Databricks does not manage thresholds so the spark code needs to be replicated
   * @param ctx
   * @param expressions
   * @return
   */
  def subexpressionEliminationForWholeStageCodegen(ctx: CodegenContext, expressions: Seq[Expression]): SubExprCodes = {
    // for oss ctx.subexpressionEliminationForWholeStageCodegen(expressions)
    // Create a clear EquivalentExpressions and SubExprEliminationState mapping
    val equivalentExpressions: EquivalentExpressions = new EquivalentExpressions
    val localSubExprEliminationExprsForNonSplit =
      mutable.HashMap.empty[ExpressionEquals, SubExprEliminationState]

    // Add each expression tree and compute the common subexpressions.
    expressions.foreach(equivalentExpressions.addExprTree(_))

    // Get all the expressions that appear at least twice and set up the state for subexpression
    // elimination.
    val commonExprs = equivalentExpressions.getCommonSubexpressions

    val nonSplitCode = {
      val allStates = mutable.ArrayBuffer.empty[SubExprEliminationState]
      commonExprs.map { expr =>
        ctx.withSubExprEliminationExprs(localSubExprEliminationExprsForNonSplit.toMap) {
          val eval = expr.genCode(ctx)
          // Collects other subexpressions from the children.
          val childrenSubExprs = mutable.ArrayBuffer.empty[SubExprEliminationState]
          expr.foreach { e =>
            ShimExprUtils.currentSubExprState(ctx).get(ExpressionEquals(e)) match {
              case Some(state) => childrenSubExprs += state
              case _ =>
            }
          }
          val state = SubExprEliminationState(eval, childrenSubExprs.toSeq)
          localSubExprEliminationExprsForNonSplit.put(ExpressionEquals(expr), state)
          allStates += state
          Seq(eval)
        }
      }
      allStates.toSeq
    }

    // For some operators, they do not require all its child's outputs to be evaluated in advance.
    // Instead it only early evaluates part of outputs, for example, `ProjectExec` only early
    // evaluate the outputs used more than twice. So we need to extract these variables used by
    // subexpressions and evaluate them before subexpressions.
    val (inputVarsForAllFuncs, exprCodesNeedEvaluate) = commonExprs.map { expr =>
      val (inputVars, exprCodes) = getLocalInputVariableValues(ctx, expr)
      (inputVars.toSeq, exprCodes.toSeq)
    }.unzip

    val needSplit = nonSplitCode.map(_.eval.code.length).sum > SQLConf.get.methodSplitThreshold
    val (subExprsMap, exprCodes) = if (needSplit) {
      if (inputVarsForAllFuncs.map(calculateParamLengthFromExprValues).forall(isValidParamLength)) {
        val localSubExprEliminationExprs =
          mutable.HashMap.empty[ExpressionEquals, SubExprEliminationState]

        commonExprs.zipWithIndex.foreach { case (expr, i) =>
          val eval = ctx.withSubExprEliminationExprs(localSubExprEliminationExprs.toMap) {
            Seq(expr.genCode(ctx))
          }.head

          val value = ctx.addMutableState(javaType(expr.dataType), "subExprValue")

          val isNullLiteral = eval.isNull match {
            case TrueLiteral | FalseLiteral => true
            case _ => false
          }
          val (isNull, isNullEvalCode) = if (!isNullLiteral) {
            val v = ctx.addMutableState(JAVA_BOOLEAN, "subExprIsNull")
            (JavaCode.isNullGlobal(v), s"$v = ${eval.isNull};")
          } else {
            (eval.isNull, "")
          }

          // Generate the code for this expression tree and wrap it in a function.
          val fnName = ctx.freshName("subExpr")
          val inputVars = inputVarsForAllFuncs(i)
          val argList =
            inputVars.map(v => s"${CodeGenerator.typeName(v.javaType)} ${v.variableName}")
          val fn =
            s"""
               |private void $fnName(${argList.mkString(", ")}) {
               |  ${eval.code}
               |  $isNullEvalCode
               |  $value = ${eval.value};
               |}
               """.stripMargin

          // Collects other subexpressions from the children.
          val childrenSubExprs = mutable.ArrayBuffer.empty[SubExprEliminationState]
          expr.foreach { e =>
            localSubExprEliminationExprs.get(ExpressionEquals(e)) match {
              case Some(state) => childrenSubExprs += state
              case _ =>
            }
          }

          val inputVariables = inputVars.map(_.variableName).mkString(", ")
          val code = code"${ctx.addNewFunction(fnName, fn)}($inputVariables);"
          val state = SubExprEliminationState(
            ExprCode(code, isNull, JavaCode.global(value, expr.dataType)),
            childrenSubExprs.toSeq)
          localSubExprEliminationExprs.put(ExpressionEquals(expr), state)
        }
        (localSubExprEliminationExprs, exprCodesNeedEvaluate)
      } else {
        val errMsg = "Failed to split subexpression code into small functions because " +
          "the parameter length of at least one split function went over the JVM limit: " +
          MAX_JVM_METHOD_PARAMS_LENGTH
        if (ShimExprUtils.isTesting) {
          throw SparkException.internalError(errMsg)
        } else {
          logInfo(errMsg)
          (localSubExprEliminationExprsForNonSplit, Seq.empty)
        }
      }
    } else {
      (localSubExprEliminationExprsForNonSplit, Seq.empty)
    }
    SubExprCodes(subExprsMap.toMap, exprCodes.flatten)
  }
}
