package com.sparkutils.shim

import org.apache.spark.sql.catalyst.analysis.{GetColumnByOrdinal, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.objects.{InitializeJavaBean, NewInstance}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Encoder, Encoders}

import scala.reflect.ClassTag
import scala.util.{Failure, Success}

object BeanEncoder {

    /**
     * Default name unification, removes _ . and - from names and lower cases
     * @param name
     * @return
     */
    def unifyName(name: String) = name.replaceAll("[_\\.-]","").toLowerCase

    /**
     * Rewrites the encoder parameter to fit the lens fields - auto renaming without config for datasets.
     *
     * It works by assuming I_AM_A_FIELD in java / scala is represented by the bean field iAmAField.  This can be extended by the unificationExtension param.
     *
     * If the field name is radically different you can use the unificationExtension to specifically look for the field (without underscores)
     *
     * Assumptions: no duplicate fields exist after removing symbols e.g. there shouldn't be both I_AM_A_FIELD and IAMAFIELD or IAM_A_FIEL_D for that matter.

     * @param readViaBean - when true the BeanEncoder will read from a normal bean encoder but output as BeanEncoder - use this for map transformations
     * @param unificationExtension provide an optional extension to handle odd field names
     * @param unificationMapExtension override and extend the Map used for field aliasing
     * @return
     */
    def beanEncoder[T: ClassTag](fields: Seq[String],
                                 unificationMapExtension: Map[String, String] = Map.empty,
                                 unificationExtension: String => String = identity, readViaBean: Boolean = false): Encoder[T] = {
      import org.apache.spark.sql.catalyst.expressions._
      val `type` = implicitly[ClassTag[T]].runtimeClass
      val scannedEnc = Encoders.bean(`type`.asInstanceOf[Class[T]]).asInstanceOf[ExpressionEncoder[T]]

      // names are already fitting bean, but lets get the encoders
      val unifiedToRealField =
        fields.map(f => unificationExtension(unifyName(f)) -> f).toMap ++ unificationMapExtension

      // go over each field and replace getters / setters:
      val deserializer = if (readViaBean) scannedEnc.deserializer else
        scannedEnc.deserializer match {
          case jbsetter: InitializeJavaBean =>
            val deserializer: Map[String, Expression] = jbsetter.setters.map { e =>
              val withOutSet = e._1.drop(3)
              val colname = withOutSet.head.toLower + withOutSet.tail
              unifiedToRealField.get(unifyName(colname)).map {
                source_field =>
                  val newExp = e._2.transform{
                    case u: UpCast =>
                      u.copy(child = UnresolvedAttribute.quoted(s"$source_field"))
                  }
                  (e._1, newExp)
              }.getOrElse(e)
            }
            jbsetter.copy(setters = deserializer)
          case newinst: NewInstance =>
            newinst
        }
      val deserializerObject = If(IsNull(GetColumnByOrdinal(0, StringType)), deserializer, deserializer)

      // serializer similar in reverse
      val serTry = scannedEnc.objSerializer match {
        case serializer: CreateNamedStruct => Success(serializer)
        case If(_: IsNull, _, serializer: CreateNamedStruct) => Success(serializer)
        case expression =>
          val err = s"customBeanEncoder: Did not get an expected ExpressionEncoder type: $expression"
          Failure(new Exception(err))
      }

      val struct = serTry.map { ser =>
          ser.children.grouped(2).flatMap {
            case Seq(a: Literal, exp: Expression) =>
              val name = a.toString // just Literal
              unifiedToRealField.get(unifyName(name)).map {
                target_field =>
                  // use the orig source
                  Seq(Literal(s"$target_field"), exp)
              }.getOrElse(Seq(a, exp))
            // normally expected to be an alias, but we shouldn't swap unless it truly is and it's a static (assumed enum)
            case Seq(n, t) => Seq(n, t) // default
          }.toSeq
        }
        .map(CreateNamedStruct)
        .get

      val serializerObject = If(IsNull(GetColumnByOrdinal(0, StringType)), struct, struct)

      val reconstructed = new ExpressionEncoder[T](
        objSerializer = serializerObject,
        objDeserializer = deserializerObject,
        implicitly[ClassTag[T]])

      reconstructed
    }
  }