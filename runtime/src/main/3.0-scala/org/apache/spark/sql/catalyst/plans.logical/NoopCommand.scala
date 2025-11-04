package org.apache.spark.sql.catalyst.plans.logical

case class NoopCommand(commandName: String, multipartIdentifier: Seq[String]) extends Command
