package com.sparkutils.shim.expressions

import org.apache.spark.sql
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Coalesce, CreateNamedStruct, CreateStruct, Expression}

import scala.annotation.nowarn

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

  @nowarn
  def sumDistinct(e: Column): Column = sql.functions.sumDistinct(e)

  @nowarn
  def shiftRightUnsigned(e: Column, numBits: Int): Column = sql.functions.shiftRightUnsigned(e, numBits)

  @nowarn
  def shiftRight(e: Column, numBits: Int): Column = sql.functions.shiftRight(e, numBits)

  @nowarn
  def shiftLeft(e: Column, numBits: Int): Column = sql.functions.shiftLeft(e, numBits)

  @nowarn
  def bitwiseNOT(e: Column): Column = sql.functions.bitwiseNOT(e)

}

object CreateStruct1 {
  def apply(children: scala.Seq[Expression]): CreateNamedStruct =
    CreateStruct.apply(children)
}