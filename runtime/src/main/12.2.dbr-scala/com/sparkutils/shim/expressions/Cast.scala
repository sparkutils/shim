package com.sparkutils.shim.expressions

import org.apache.spark.sql.catalyst.expressions.{Add, Cast, Expression}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.catalyst.expressions.EvalMode

object Cast2 {

  /**
   * Dbr 11.2 broke the contract for add and cast
   *
   * @param child
   * @param dataType
   * @return
   */
  def apply(child: Expression, dataType: DataType): Expression =
    Cast(child, dataType)

  def unapply(cast: Expression): Option[(Expression, DataType)] =
    cast match {
      case Cast(ch, dt, _, _) =>
        Some((ch, dt))
      case _ => None
    }
}

object Cast3 {

  /**
   * Dbr 11.2 broke the contract for add and cast
   *
   * @param child
   * @param dataType
   * @return
   */
  def apply(child: Expression, dataType: DataType, timeZoneId: Option[String] = None): Expression =
    Cast(child, dataType, timeZoneId)

  def unapply(cast: Expression): Option[(Expression, DataType, Option[String])] =
    cast match {
      case Cast(ch, dt, tz, _) =>
        Some((ch, dt, tz))
      case _ => None
    }
}

object Cast4 {

  /**
   * Dbr 11.2 broke the contract for add and cast
   *
   * @param child
   * @param dataType
   * @return
   */
  def apply(child: Expression, dataType: DataType, timeZoneId: Option[String] = None, evalMode: EvalMode.Value = EvalMode.fromSQLConf(SQLConf.get)): Expression =
    Cast(child, dataType, timeZoneId)

  def unapply(cast: Expression): Option[(Expression, DataType, Option[String], EvalMode.Value)] =
    cast match {
      case Cast(ch, dt, tz, ansiEnabled) =>
        Some((ch, dt, tz,
          if (ansiEnabled)
            EvalMode.ANSI
          else
            EvalMode.LEGACY
        ))
      case _ => None
    }
}