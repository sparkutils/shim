package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, GetColumnByOrdinal, TypeCheckResult, UnresolvedFunction, UnresolvedRelation}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.{Add, Attribute, BoundReference, Cast, CreateNamedStruct, DecimalAddNoOverflowCheck, Expression, ExpressionInfo, GetArrayStructFields, GetStructField, If, Literal, PrettyAttribute, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, FunctionIdentifier}
import org.apache.spark.sql.execution.{QueryExecution, SparkSqlParser}
import org.apache.spark.sql.shim.hash.{Digest, InterpretedHashLongsFunction}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

import java.util.Locale
import scala.reflect.ClassTag

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
   * Optimise add's for decimal / DBR 11.2 add interface change.  Per Sum/Avg Decimals have their own add without overflow checks for performance gains
   * @param left
   * @param right
   * @return
   */
  def add(left: Expression, right: Expression, dataType: DataType): Expression =
    if ((dataType ne null) && dataType.isInstanceOf[DecimalType])
      DecimalAddNoOverflowCheck(left, right, dataType)
    else
      Add(left, right)

  /**
   * Dbr 11.2 broke the contract for add and cast
   * @param child
   * @param dataType
   * @return
   */
  def cast(child: Expression, dataType: DataType): Expression =
    Cast(child, dataType)

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
    TypeCheckResult.TypeCheckFailure(s"$errorSubClass extra info - $messageParameters")

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
  def sparkOrdering(dataType: DataType): Ordering[_] = dataType.asInstanceOf[AtomicType].ordering

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

  // Below are added for Frameless RowEncoder, TypedEncoder and TypedExpressionEncoder support
  def targetStructType(dataType: DataType, nullable: Boolean): StructType =
    dataType match {
      case x: StructType =>
        if (nullable) StructType(x.fields.map(_.copy(nullable = true)))
        else x

      case dt => new StructType().add("value", dt, nullable = nullable)
    }

  def expressionEncoder[T: ClassTag](jvmRepr: DataType, nullable: Boolean, toCatalyst: Expression => Expression, catalystRepr: DataType, fromCatalyst: Expression => Expression): Encoder[T] = {
    val in = BoundReference(0, jvmRepr, nullable)

    val (out, serializer) = toCatalyst(in) match {
      case it @ If(_, _, _: CreateNamedStruct) => {
        val out = GetColumnByOrdinal(0, catalystRepr)

        out -> it
      }

      case other => {
        val out = GetColumnByOrdinal(0, catalystRepr)

        out -> other
      }
    }

    new ExpressionEncoder[T](
      objSerializer = serializer,
      objDeserializer = fromCatalyst(out),
      clsTag = implicitly[ClassTag[T]]
    )
  }

  def analysisException(ds: Dataset[_], colNames: Seq[String]): AnalysisException =
    new AnalysisException( s"""Cannot resolve column name "$colNames" among (${ds.schema.fieldNames.mkString(", ")})""" )

  def executePlan(ds: Dataset[_], plan: LogicalPlan): QueryExecution =
    ds.sparkSession.sessionState.executePlan(plan)

  /**
   * 4 preview2 introduces ColumnNode and hides Expression - ExpressionUtils provides wrapping function
   * @param expression
   * @return
   */
  def column(expression: Expression): Column = new Column(expression)

  /**
   * 4 preview2 moves named to ExpressionUtils
   * @param expression
   * @return
   */
  def toNamed(col: Column): NamedExpression = col.named

  /**
   * 4 preview2 introduces ColumnNode and hides Expression - ExpressionUtils provides unwrapping function
   * @param expression
   * @return
   */
  def expression(column: Column): Expression = column.expr

  /**
   * Agnostic encoders in 4 preview2 are used which forces new serializers to be created instead of using those in the encoders
   * This version introduces the same functionality using a provided encoder (e.g. one from frameless which doesn't do this).
   *
   * @param current
   * @param other
   * @param condition
   * @param joinType
   * @param enc
   * @tparam T
   * @tparam U
   * @return
   */
  def joinWith[T, U](current: Dataset[T], other: Dataset[U], condition: Column, joinType: String)(implicit enc: Encoder[(T,U)]): Dataset[(T, U)] =
    current.joinWith(other, condition, joinType)
}
