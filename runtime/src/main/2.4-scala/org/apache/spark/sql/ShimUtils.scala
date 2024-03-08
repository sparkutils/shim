package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, GetColumnByOrdinal, TypeCheckResult, UnresolvedFunction, UnresolvedRelation}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.{Add, Attribute, BoundReference, Cast, CreateNamedStruct, Expression, GetArrayStructFields, GetStructField, If, Literal, PrettyAttribute}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf
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
      unresolvedFunction.children

    def withArguments(children: Seq[Expression]): UnresolvedFunction =
      unresolvedFunction.copy(children = children)
  }

  def isPrimitive(dataType: DataType) =
    dataType match {
      case BooleanType => true
      case ByteType => true
      case ShortType => true
      case IntegerType => true
      case LongType => true
      case FloatType => true
      case DoubleType => true
      case _ => false
    }

  /**
   * Arguments for everything above 2.4
   */
  def arguments(unresolvedFunction: UnresolvedFunction): Seq[Expression] =
    unresolvedFunction.children

  /**
   * Dbr 11.2 broke the contract for add and cast
   * @param left
   * @param right
   * @return
   */
  def add(left: Expression, right: Expression, dataType: DataType): Expression =
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

  /**
   * Provide spark2 specific version of hashing CalendarInterval
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
    new SparkSqlParser(new SQLConf)
  }

  /**
   * Registers functions with spark, Introduced in 0.4 - 3.2.0 support due to extra source parameter - "built-in" is used as no other option is remotely close
   *
   * @param funcReg
   * @param name
   * @param builder
   */
  def registerFunction(funcReg: FunctionRegistry)(name: String, builder: Seq[Expression] => Expression) =
    funcReg.createOrReplaceTempFunction(name, builder)

  /**
   * Used by the SparkSessionExtensions mechanism, but 2.4 doesn't support this so it's a no-op
   * @param extensions
   * @param name
   * @param builder
   */
  def registerFunctionViaExtension(extensions: SparkSessionExtensions)(name: String, builder: Seq[Expression] => Expression) =
    ()

  /**
   * Used by the SparkSessionExtensions mechanism but registered via builtin registry
   * @param name
   * @param builder
   */
  def registerFunctionViaBuiltin(name: String, builder: Seq[Expression] => Expression) =
    ()

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

  def targetStructType(dataType: DataType, nullable: Boolean): StructType =
    dataType match {
      case x: StructType =>
        if (nullable) StructType(x.fields.map(_.copy(nullable = true)))
        else x

      case dt => new StructType().add("_1", dt, nullable = nullable)
    }

  def expressionEncoder[T: ClassTag](jvmRepr: DataType, nullable: Boolean, toCatalyst: Expression => Expression, catalystRepr: DataType, fromCatalyst: Expression => Expression): Encoder[T] = {
    val schema = targetStructType(catalystRepr, nullable)
    val in = BoundReference(0, jvmRepr, nullable)

    val (out, toRowExpressions) = toCatalyst(in) match {
      case If(_, _, x: CreateNamedStruct) =>
        val out = BoundReference(0, catalystRepr, nullable)

        (out, x.flatten)
      case other =>
        val out = GetColumnByOrdinal(0, catalystRepr)

        (out, CreateNamedStruct(Literal("_1") :: other :: Nil).flatten)
    }

    new ExpressionEncoder[T](
      schema = schema,
      flat = false,
      serializer = toRowExpressions,
      deserializer = fromCatalyst(out),
      clsTag = implicitly[ClassTag[T]]
    )
  }

  def analysisException(ds: Dataset[_], colNames: Seq[String]): AnalysisException =
    new AnalysisException( s"""Cannot resolve column name "$colNames" among (${ds.schema.fieldNames.mkString(", ")})""" )
}