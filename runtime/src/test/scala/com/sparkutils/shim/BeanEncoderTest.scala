package com.sparkutils.shim

import org.apache.spark.sql.Encoders.bean
import org.apache.spark.sql.{Encoder, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class BeanEncoderTest extends AnyFunSpec with Matchers {

  describe("Bean encoder wrapper") {
    it("should convert soft lens to usable fields") {
      testIAmField("i_Am_Field",
        fields => BeanEncoder.beanEncoder(fields) )      
    }
    it("should allow extension of string handling") {
      testIAmField("i.Am.Field",
        fields => BeanEncoder.beanEncoder( fields,
          unificationExtension = _.replaceAll("\\.","")) )
    }
    it("should allow overriding of map") {
      val notAField = "i.Am_not_A.Field"
      testIAmField(notAField,
        fields => BeanEncoder.beanEncoder( fields,
          unificationMapExtension = Map("iamfield" -> notAField)) )
    }
  }

  val spark = SparkSession.builder.
    config("spark.ui.enabled", "false").
    config("SPARK_USER","usr").
    master("local[1]").getOrCreate

  def testIAmField(iamFieldName: String, beanFunc: Seq[String] => Encoder[TheBean]) = {
    val starter = spark.sql(s"select 'iamfield' as `$iamFieldName`, 'alsoafield' as al_so_afield")
    val fields = Seq(
      iamFieldName,
      "al_so_afield"
    )
    implicit val newenc = beanFunc(fields)

    val instance = starter.as[TheBean].first

    assert("iamfield" === instance.iAmField)
    assert("alsoafield" === instance.also_a_field)

    import spark.implicits._
    val reread = Seq(instance).toDS.toDF.schema

    assert(reread.fields.toSet ===
      Set(StructField("al_so_afield",StringType,true),
        StructField(iamFieldName,StringType,true)
      ))
  }

  describe("ReadViaBean") {
    it("Bean encoder to map to BeanEncoder should work") {
      import spark.implicits._

      val tbean = new TheBean
      tbean.also_a_field = "yes"
      tbean.iAmField = "no"
      val ds = {
        implicit val enc = bean(classOf[TheBean])
        Seq(tbean).toDS
      }
      // ds is now in bean encoder, let's map it again
      val mapped =
        ds.map{b =>
          b.iAmField = "yes"
          b
        }(bean(classOf[TheBean]))

      val converted = {
        implicit val enc = BeanEncoder.beanEncoder[TheBean](
          Seq(
            "i.am.Field",
            "al_so_afield"
          ), unificationMapExtension = Map("iamfield" -> "i.am.Field"),
          readViaBean = true) // by default false and the test will fail on the map
        mapped.as[TheBean]
      }

      val res = converted.toDF.filter("iAmField = also_a_field")
      assert(res.count === 1)
    }
  }
}