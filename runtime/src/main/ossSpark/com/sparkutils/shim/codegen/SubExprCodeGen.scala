package com.sparkutils.shim.codegen

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, SubExprCodes}

object SubExprCodeGen extends Logging {
  /**
   * Databricks does not manage thresholds so the spark code needs to be replicated
   * @param ctx
   * @param expressions
   * @return
   */
  def subexpressionEliminationForWholeStageCodegen(ctx: CodegenContext, expressions: Seq[Expression]): SubExprCodes =
    ctx.subexpressionEliminationForWholeStageCodegen(expressions)
}
