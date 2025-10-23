package com.sparkutils.shim.expressions

import org.apache.spark.sql
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Coalesce, CreateNamedStruct, CreateStruct, Expression}

// TODO add back in after dropping 2.4 / 2.11
// import scala.annotation.nowarn

/*
 * Collection of shims on really stable interfaces for functional expressions
 */

object Coalesce1 {
  def apply(seq: scala.Seq[Expression]): Coalesce =
    Coalesce(seq)

  def unapply(coalesce: Coalesce): Option[scala.Seq[Expression]] =
    Coalesce.unapply(coalesce)
}

// frameless has 5 deprecated functions which are now errors, but for source compat they need to stay, no idea as to when they will
// be removed, shim later..
object functions {

  def sumDistinct(e: Column): Column = sql.functions.sumDistinct(e)

  def shiftRightUnsigned(e: Column, numBits: Int): Column = sql.functions.shiftRightUnsigned(e, numBits)

  def shiftRight(e: Column, numBits: Int): Column = sql.functions.shiftRight(e, numBits)

  def shiftLeft(e: Column, numBits: Int): Column = sql.functions.shiftLeft(e, numBits)

  def bitwiseNOT(e: Column): Column = sql.functions.bitwiseNOT(e)

}

object CreateStruct1 {
  def apply(children: scala.Seq[Expression]): CreateNamedStruct =
    CreateStruct.apply(children)
}