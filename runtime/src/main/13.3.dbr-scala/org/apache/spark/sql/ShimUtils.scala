package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, TypeCheckResult, UnresolvedFunction, UnresolvedRelation}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Add, Attribute, BinaryOperator, Cast, Expression, ExpressionInfo, GetArrayStructFields, GetStructField, Literal, PrettyAttribute}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, FunctionIdentifier}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.shim.hash.{Digest, InterpretedHashLongsFunction}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import java.util.Locale

/**
 * 3.4 backport present on databricks 11.3 lts
 * An add expression for decimal values which is only used internally by Sum/Avg.
 *
 * Nota that, this expression does not check overflow which is different with `Add`. When
 * aggregating values, Spark writes the aggregation buffer values to `UnsafeRow` via
 * `UnsafeRowWriter`, which already checks decimal overflow, so we don't need to do it again in the
 * add expression used by Sum/Avg.
 */
case class QDecimalAddNoOverflowCheck(
                                       left: Expression,
                                       right: Expression,
                                       override val dataType: DataType) extends BinaryOperator {
  require(dataType.isInstanceOf[DecimalType])

  override def inputType: AbstractDataType = DecimalType
  override def symbol: String = "+"
  private def decimalMethod: String = "$plus"

  private lazy val numeric = TypeUtils.getNumeric(dataType)

  override protected def nullSafeEval(input1: Any, input2: Any): Any =
    numeric.plus(input1, input2)

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    defineCodeGen(ctx, ev, (eval1, eval2) => s"$eval1.$decimalMethod($eval2)")

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): QDecimalAddNoOverflowCheck =
    copy(left = newLeft, right = newRight)
}

/**
 * Set of utilities to reach in to private functions
 */
object ShimUtils {
  implicit class UnresolvedFunctionOps(unresolvedFunction: UnresolvedFunction) {

    def theArguments: Seq[Expression] =
      unresolvedFunction.arguments

    def withArguments(children: Seq[Expression]): UnresolvedFunction =
      unresolvedFunction.copy(arguments = children)
  }

  def isPrimitive(dataType: DataType) = CatalystTypeConverters.isPrimitive(dataType)

  /**
   * Arguments for everything above 2.4
   */
  def arguments(unresolvedFunction: UnresolvedFunction): Seq[Expression] =
    unresolvedFunction.arguments

  /**
   * Dbr 11.2 broke the contract for add and cast, OSS 3.4 39316 changes Add's behaviour adding silent overflows
   * @param left
   * @param right
   * @return
   */
  def add(left: Expression, right: Expression, dataType: DataType): Expression =
    if ((dataType ne null) && dataType.isInstanceOf[DecimalType])
      QDecimalAddNoOverflowCheck(left, right, dataType)
    else
      new Add(left, right)

  /**
   * Dbr 11.2 broke the contract for add and cast
   * @param child
   * @param dataType
   * @return
   */
  def cast(child: Expression, dataType: DataType): Expression =
    new Cast(child, dataType, None)

  /**
   * Provides Spark 3 specific version of hashing CalendarInterval
   *
   * @param c
   * @param hashlongs
   * @param digest
   * @return
   */
  def hashCalendarInterval(c: CalendarInterval, hashlongs: InterpretedHashLongsFunction, digest: Digest): Digest = {
    import hashlongs._
    hashInt(c.months, hashLong(c.microseconds, digest))
  }

  /**
   * Creates a new parser, introduced in 0.4 - 3.2.0 due to SparkSqlParser having no params
   * @return
   */
  def newParser() = {
    new SparkSqlParser()
  }

  /**
   * Registers functions with spark, Introduced in 0.4 - 3.2.0 support due to extra source parameter - "built-in" is used as no other option is remotely close
   *
   * @param funcReg
   * @param name
   * @param builder
   */
  def registerFunction(funcReg: FunctionRegistry)(name: String, builder: Seq[Expression] => Expression) =
    funcReg.createOrReplaceTempFunction(name, builder, "built-in")

  /**
   * Used by the SparkSessionExtensions mechanism
   * @param extensions
   * @param name
   * @param builder
   */
  def registerFunctionViaExtension(extensions: SparkSessionExtensions)(name: String, builder: Seq[Expression] => Expression) =
    extensions.injectFunction( (FunctionIdentifier(name), new ExpressionInfo(name, name) , builder) )

  /**
   * Used by the SparkSessionExtensions mechanism but registered via builtin registry
   * @param name
   * @param builder
   */
  def registerFunctionViaBuiltin(name: String, builder: Seq[Expression] => Expression) =
    FunctionRegistry.builtin.registerFunction( FunctionIdentifier(name), new ExpressionInfo(name, name) , builder)

  /**
   * Type signature changed for 3.4 to more detailed setup, 12.2 already uses it
   * @param errorSubClass
   * @param messageParameters
   * @return
   */
  def mismatch(errorSubClass: String, messageParameters: Map[String, String]): TypeCheckResult =
    DataTypeMismatch(
      errorSubClass = errorSubClass,
      messageParameters = messageParameters
    )

  def toSQLType(t: AbstractDataType): String = t match {
    case TypeCollection(types) => types.map(toSQLType).mkString("(", " or ", ")")
    case dt: DataType => quoteByDefault(dt.sql)
    case at => quoteByDefault(at.simpleString.toUpperCase(Locale.ROOT))
  }
  def toSQLExpr(e: Expression): String = {
    quoteByDefault(toPrettySQL(e))
  }

  def usePrettyExpression(e: Expression): Expression = e transform {
    case a: Attribute => new PrettyAttribute(a)
    case Literal(s: UTF8String, StringType) => PrettyAttribute(s.toString, StringType)
    case Literal(v, t: NumericType) if v != null => PrettyAttribute(v.toString, t)
    case Literal(null, dataType) => PrettyAttribute("NULL", dataType)
    case e: GetStructField =>
      val name = e.name.getOrElse(e.childSchema(e.ordinal).name)
      PrettyAttribute(usePrettyExpression(e.child).sql + "." + name, e.dataType)
    case e: GetArrayStructFields =>
      PrettyAttribute(usePrettyExpression(e.child) + "." + e.field.name, e.dataType)
    case c: Cast =>
      PrettyAttribute(usePrettyExpression(c.child).sql, c.dataType)
  }

  def toPrettySQL(e: Expression): String = usePrettyExpression(e).sql
  // Converts an error class parameter to its SQL representation
  def toSQLValue(v: Any, t: DataType): String = Literal.create(v, t) match {
    case Literal(null, _) => "NULL"
    case Literal(v: Float, FloatType) =>
      if (v.isNaN) "NaN"
      else if (v.isPosInfinity) "Infinity"
      else if (v.isNegInfinity) "-Infinity"
      else v.toString
    case l @ Literal(v: Double, DoubleType) =>
      if (v.isNaN) "NaN"
      else if (v.isPosInfinity) "Infinity"
      else if (v.isNegInfinity) "-Infinity"
      else l.sql
    case l => l.sql
  }

  private def quoteByDefault(elem: String): String = {
    "\"" + elem + "\""
  }

  // https://issues.apache.org/jira/browse/SPARK-43019 in 3.5, backported to 13.1 dbr
  def sparkOrdering(dataType: DataType): Ordering[_] = PhysicalDataType.ordering(dataType)

  def tableOrViewNotFound(e: Exception): Option[Either[Exception, Set[String]]] =
    e match {
      case ae: AnalysisException =>
        Some(ae.plan.fold[Either[Exception, Set[String]]]{
          // spark 2.4 just has exception: Table or view not found: names
          if (ae.message.contains("Table or view not found"))
            Right(Set(ae.message.split(":")(1).trim))
          else
            Left(ae)
        } {
          plan =>
            val c =
              plan.collect {
                case ur: UnresolvedRelation =>
                  ur.tableName
              }

            if (c.isEmpty)
              Left(ae) // not what we expected
            else
              Right(c.toSet)
        })
      case _ => None
    }

  def rowEncoder(structType: StructType) = RowEncoder(structType)
}
