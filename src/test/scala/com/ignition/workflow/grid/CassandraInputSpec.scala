package com.ignition.workflow.grid

import org.joda.time.{ DateTime, DateTimeZone }
import org.junit.runner.RunWith
import org.specs2.matcher.XmlMatchers
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import com.eaio.uuid.UUID
import com.ignition.{ CassandraSpec, SparkTestHelper }
import com.ignition.data.{ Decimal, DefaultRowMetaData }
import com.ignition.data.DataType.{ BooleanDataType, DateTimeDataType, DecimalDataType, DoubleDataType, IntDataType, StringDataType, UUIDDataType }
import com.ignition.workflow.rdd.grid.input.CassandraInput

@RunWith(classOf[JUnitRunner])
class CassandraInputSpec extends Specification with XmlMatchers with CassandraSpec with SparkTestHelper {
  sequential

  val keySpace = "ignition"
  val dataSet = "ignition_test.ddl"

  override def afterAll = {
    super[CassandraSpec].afterAll
    super[SparkTestHelper].afterAll
  }

  val meta = DefaultRowMetaData.create.add[UUID]("customer_id")
    .add[String]("description").add[DateTime]("date").add[Decimal]("total")
    .add[Int]("items").add[Double]("weight").add[Boolean]("shipped")

  "CassandraInput" should {
    "load data without filtering" in {
      val input = CassandraInput("ignition", "orders", meta)
      input.outMetaData === Some(meta)
      input.output.count === 8
      val rows = input.output.filter(
        _.getUUID("customer_id") == new UUID("c7b44cb2-b6bf-11e4-a71e-12e3f512a338")).collect
      rows.size === 1
      val row = rows.head
      row.getString("description") === "furniture"
      row.getDateTime("date").withZone(DateTimeZone.UTC) === date(2015, 1, 5)
      row.getDecimal("total") === BigDecimal(620)
      row.getInt("items") === 1
      row.getDouble("weight") === 650.0
      row.getBoolean("shipped") === true
    }
    "filter data by partitioning columns" in {
      val input = CassandraInput("ignition", "orders", meta, "customer_id=?",
        new UUID("d3fdcf34-b6bf-11e4-a71e-12e3f512a338"))
      input.output.count === 2
    }
    "filter data by partitioning and clustering columns" in {
      val input = CassandraInput("ignition", "orders", meta,
        "customer_id=? and date>=? and date<=?",
        new UUID("c7b44500-b6bf-11e4-a71e-12e3f512a338"),
        date(2015, 1, 8), date(2015, 1, 20))
      input.output.count === 1
    }
    "filter data by partitioning, clustering, and indexed columns" in {
      val input = CassandraInput("ignition", "orders", meta,
        "customer_id=? and date>=? and date<=? and description=?",
        new UUID("c7b44500-b6bf-11e4-a71e-12e3f512a338"),
        date(2015, 1, 1), date(2015, 3, 1), "clothes")
      input.output.count === 2
    }
    "save to xml/load from xml" in {
      val input = CassandraInput("ignition", "orders", meta,
        "customer_id=? and date>=? and date<=? and description=?",
        new UUID("c7b44500-b6bf-11e4-a71e-12e3f512a338"),
        date(2015, 1, 1), date(2015, 3, 1), "clothes")
      val xml = input.toXml
      val input2 = CassandraInput.fromXml(xml)
      input === input2
    }
  }

  protected def date(year: Int, month: Int, day: Int) =
    new DateTime(year, month, day, 0, 0).withZone(DateTimeZone.UTC)
}