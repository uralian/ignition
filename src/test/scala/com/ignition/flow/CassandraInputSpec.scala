package com.ignition.flow

import java.io.{ ByteArrayOutputStream, IOException, ObjectOutputStream }

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.types.Decimal
import org.junit.runner.RunWith
import org.specs2.matcher.XmlMatchers
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import com.ignition.{ CassandraSpec, SparkTestHelper }
import com.ignition.types._

@RunWith(classOf[JUnitRunner])
class CassandraInputSpec extends Specification with CassandraSpec with XmlMatchers with SparkTestHelper {
  import ctx.implicits._

  sequential

  val keySpace = "ignition"
  val dataSet = "ignition_test.ddl"

  override def afterAll = {
    super[CassandraSpec].afterAll
    super[SparkTestHelper].afterAll
  }

  val schema = string("customer_id", false) ~ string("description") ~ date("date", false) ~
    decimal("total") ~ int("items") ~ double("weight") ~ boolean("shipped")

  "CassandraInput" should {
    "load data without filtering" in {
      val cass = CassandraInput("ignition", "orders", schema)
      cass.outputSchema === Some(schema)
      val output = cass.output
      output.count === 8
      val row = output.filter("customer_id = 'c7b44cb2-b6bf-11e4-a71e-12e3f512a338'").first
      row(0) === "c7b44cb2-b6bf-11e4-a71e-12e3f512a338"
      row(1) === "furniture"
      row(2) === javaDate(2015, 1, 5)
      row(3) === Decimal("620.00").toJavaBigDecimal
      row(4) === 1
      row(5) === 650.0
      row(6) === true
    }
    "filter data by partitioning columns" in {
      val input = CassandraInput("ignition", "orders", schema, "customer_id=?",
        "d3fdcf34-b6bf-11e4-a71e-12e3f512a338")
      input.output.count === 2
    }
    "filter data by partitioning and clustering columns" in {
      val input = CassandraInput("ignition", "orders", schema,
        "customer_id=? and date>=? and date<=?",
        "c7b44500-b6bf-11e4-a71e-12e3f512a338",
        javaDate(2015, 1, 8), javaDate(2015, 1, 20))
      input.output.count === 1
    }
    "filter data by partitioning, clustering, and indexed columns" in {
      val input = CassandraInput("ignition", "orders", schema,
        "customer_id=? and date>=? and date<=? and description=?",
        "c7b44500-b6bf-11e4-a71e-12e3f512a338", javaDate(2015, 1, 1), javaDate(2015, 3, 1), "clothes")
      input.output.count === 2
    }
    "save to xml/load from xml" in {
      val input = CassandraInput("ignition", "orders", schema,
        "customer_id=? and date>=? and date<=? and description=?",
        "c7b44500-b6bf-11e4-a71e-12e3f512a338", javaDate(2015, 1, 1), javaDate(2015, 3, 1), "clothes")
      val xml = input.toXml
      val input2 = CassandraInput.fromXml(xml)
      input === input2
    }
    "be unserializable" in {
      val cass = CassandraInput("ignition", "orders", schema)
      val oos = new ObjectOutputStream(new ByteArrayOutputStream())
      oos.writeObject(cass) must throwA[IOException]
    }
  }

  protected def javaDate(year: Int, month: Int, day: Int) = java.sql.Date.valueOf(s"$year-$month-$day")
}