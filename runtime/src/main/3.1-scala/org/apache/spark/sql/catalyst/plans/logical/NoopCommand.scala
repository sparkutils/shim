package org.apache.spark.sql.catalyst.plans.logical

case class NoopCommand(commandName: String, multipartIdentifier: scala.Seq[String]) extends Command
