package org.apache.spark.sql.execution

import org.apache.spark.sql.internal.SQLConf

class SparkSqlParser() extends _root_.org.apache.spark.sql.catalyst.parser.AbstractSqlParser {
  val astBuilder: _root_.org.apache.spark.sql.execution.SparkSqlAstBuilder = ???

  override protected def parse[T](command: _root_.scala.Predef.String)(toResult: _root_.org.apache.spark.sql.catalyst.parser.SqlBaseParser => T): T = ???
}