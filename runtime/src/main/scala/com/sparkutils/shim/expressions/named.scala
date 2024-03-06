package com.sparkutils.shim.expressions

import org.apache.spark.sql.catalyst.expressions.{Alias, ExprId, Expression, NamedExpression}
import org.apache.spark.sql.types.Metadata

/*
 * Collection of shims on really stable interfaces for named expressions
 * Alias is stable after version 313
 */
