package org.apache.spark.sql.catalyst.catalog

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{FakeSystemCatalog, ResolvedIdentifier}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.connector.catalog.CatalogManager.{SESSION_NAMESPACE, SYSTEM_CATALOG_NAME}
import org.apache.spark.sql.connector.catalog.{CatalogManager, Identifier}
import org.apache.spark.sql.errors.DataTypeErrorsBase
import org.apache.spark.sql.errors.QueryCompilationErrors.unresolvedVariableError

import javax.annotation.concurrent.GuardedBy
import scala.collection.mutable

trait VariableDefinitionBase {
  val identifier: org.apache.spark.sql.connector.catalog.Identifier
  val currentValue: org.apache.spark.sql.catalyst.expressions.Literal
  val defaultValueSQL: String
}

case class VariableDefinition(
                               identifier: Identifier,
                               defaultValueSQL: String,
                               currentValue: Literal) extends VariableDefinitionBase

/**
 * Trait which provides an interface for variable managers. Methods are case sensitive regarding
 * the variable name/nameParts/identifier, callers are responsible to
 * format them w.r.t. case-sensitive config.
 */
trait VariableManager {
  /**
   * Create a variable.
   * @param nameParts NameParts of the variable.
   * @param varDef The VariableDefinition of the variable.
   * @param overrideIfExists If true, the new variable will replace an existing one
   *                         with the same identifier, if it exists.
   */
  def create(nameParts: Seq[String], varDef: VariableDefinitionBase, overrideIfExists: Boolean): Unit

  /**
   * Set an existing variable to a new value.
   *
   * @param nameParts Name parts of the variable.
   * @param varDef The new VariableDefinition of the variable.
   */
  def set(nameParts: Seq[String], varDef: VariableDefinition): Unit

  /**
   * Get an existing variable.
   *
   * @param nameParts Name parts of the variable.
   */
  def get(nameParts: Seq[String]): Option[VariableDefinition]

  /**
   * Delete an existing variable.
   *
   * @param nameParts Name parts of the variable.
   */
  def remove(nameParts: Seq[String]): Boolean

  /**
   * Create an identifier for the provided variable name. Could be context dependent.
   * @param name Name for which an identifier is created.
   */
  def qualify(name: String): ResolvedIdentifier

  /**
   * Delete all variables.
   */
  def clear(): Unit

  /**
   * @return true if at least one variable exists, false otherwise.
   */
  def isEmpty: Boolean

  /**
   *
   * @param variableName Name of the variable
   * @return variable name formatting for the error
   */
  def getVariableNameForError(variableName: String): String

}

/**
 * A thread-safe manager for temporary SQL variables (that live in the schema `SYSTEM.SESSION`),
 * providing atomic operations to manage them, e.g. create, get, remove, etc.
 *
 * Note that, the variable name is always case-sensitive here, callers are responsible to format the
 * variable name w.r.t. case-sensitive config.
 */
class TempVariableManager extends VariableManager with DataTypeErrorsBase {

  @GuardedBy("this")
  private val variables = new mutable.HashMap[String, VariableDefinition]

  override def create(
                       nameParts: Seq[String],
                       varDef: VariableDefinitionBase,
                       overrideIfExists: Boolean): Unit = synchronized {
  }

  override def set(nameParts: Seq[String], varDef: VariableDefinition): Unit = synchronized {
    val name = nameParts.last
    // Sanity check as this is already checked in ResolveSetVariable.
    if (!variables.contains(name)) {
      throw unresolvedVariableError(nameParts, Seq("SYSTEM", "SESSION"))
    }
    variables.put(name, varDef)
  }

  override def get(nameParts: Seq[String]): Option[VariableDefinition] = synchronized {
    variables.get(nameParts.last)
  }

  override def remove(nameParts: Seq[String]): Boolean = synchronized {
    variables.remove(nameParts.last).isDefined
  }

  override def qualify(name: String): ResolvedIdentifier =
    ResolvedIdentifier(
      FakeSystemCatalog,
      Identifier.of(Array(CatalogManager.SESSION_NAMESPACE), name)
    )

  override def clear(): Unit = synchronized {
    variables.clear()
  }

  override def isEmpty: Boolean = synchronized {
    variables.isEmpty
  }

  /**
   *
   * @param variableName Name of the variable
   * @return variable name formatting for the error
   */
  override def getVariableNameForError(variableName: String): String = variableName
}
