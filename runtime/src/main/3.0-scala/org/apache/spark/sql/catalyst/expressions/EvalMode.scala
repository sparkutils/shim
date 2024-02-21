package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.internal.SQLConf

/**
 * Copied from later Spark versions
 * Expression evaluation modes.
 *   - LEGACY: the default evaluation mode, which is compliant to Hive SQL.
 *   - ANSI: a evaluation mode which is compliant to ANSI SQL standard.
 *   - TRY: a evaluation mode for `try_*` functions. It is identical to ANSI evaluation mode
 *          except for returning null result on errors.
 */

object EvalMode extends Enumeration {
  val LEGACY, ANSI, TRY = Value

  def fromSQLConf(conf: SQLConf): Value =
    LEGACY

  def fromBoolean(ansiEnabled: Boolean): Value = if (ansiEnabled) {
    ANSI
  } else {
    LEGACY
  }
}
