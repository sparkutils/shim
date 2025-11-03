package org.apache.spark.sql.catalyst.plans.logical.Compat

import org.apache.spark.sql.catalyst.plans.logical.Command

case class NoopCommand(commandName: String, multipartIdentifier: Seq[String]) extends Command
