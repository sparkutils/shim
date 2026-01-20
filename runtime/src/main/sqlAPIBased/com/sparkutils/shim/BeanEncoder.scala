package com.sparkutils.shim

import org.apache.spark.sql.catalyst.analysis.{GetColumnByOrdinal, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.JavaBeanEncoder
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
      // AgnosticEncoder
      val scannedEnc = Encoders.bean(`type`.asInstanceOf[Class[T]])

      // names are already fitting bean, but lets get the encoders
      val unifiedToRealField =
        fields.map(f => unificationExtension(unifyName(f)) -> f).toMap ++ unificationMapExtension

      if (readViaBean)
        scannedEnc
      else
        scannedEnc match {
          case j: JavaBeanEncoder[T] =>
            j.copy( fields =
              j.fields.map {
                f =>
                  unifiedToRealField.get(unifyName(f.name)).map{ correctedName =>
                    f.copy(name = correctedName)
                  }.getOrElse(f)
              }
            )
          case _ => scannedEnc
        }
    }
  }