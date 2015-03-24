package com.ignition.flow

import java.io.{ ByteArrayOutputStream, IOException, ObjectOutputStream }

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.types.Decimal
import org.junit.runner.RunWith
import org.specs2.matcher.XmlMatchers
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import com.eaio.uuid.UUID
import com.ignition.{ CassandraSpec, SparkTestHelper }
import com.ignition.types.{ RichStructType, decimal, double, fieldToStruct, int, string, timestamp }

@RunWith(classOf[JUnitRunner])
class CassandraOutputSpec extends Specification with CassandraSpec with XmlMatchers with SparkTestHelper {
  import ctx.implicits._

  sequential

  val keySpace = "ignition"
  val dataSet = "ignition_test.ddl"

  override def afterAll = {
    super[CassandraSpec].afterAll
    super[SparkTestHelper].afterAll
  }

  val schema = string("customer_id") ~ timestamp("date") ~ decimal("total") ~ int("items") ~ double("weight")
  val id = new UUID

  "CassandraOutput" should {
    "save data to Cassandra" in {
      val grid = DataGrid(schema)
        .addRow(id.toString, javaTime(2015, 3, 11, 12, 30), Decimal(123.45), 3, 9.23)
        .addRow(id.toString, javaTime(2015, 3, 11, 17, 15), Decimal(650.0), 1, 239.0)
      val cass = CassandraOutput(keySpace, "shipments")
      grid --> cass
      cass.output
      Thread.sleep(100)
      val rows = session.execute(s"select * from shipments where customer_id=${id}").all
      rows.size === 2
      rows.get(0).getUUID("customer_id") === java.util.UUID.fromString(id.toString)
      rows.get(0).getDate("date") === javaTime(2015, 3, 11, 12, 30)
      rows.get(0).getInt("items") === 3
      rows.get(0).getDecimal("total") === javaBD(123.45)
      rows.get(0).getDouble("weight") === 9.23
      success
    }
    "save to xml" in {
      val cass = CassandraOutput("keyspace", "table")
      <cassandra-output keyspace="keyspace" table="table"/> must ==/(cass.toXml)
    }
    "load from xml" in {
      val xml = <cassandra-output keyspace="keyspace" table="table"/>
      CassandraOutput.fromXml(xml) === CassandraOutput("keyspace", "table")
    }
    "be unserializable" in {
      val cass = CassandraInput("ignition", "orders", schema)
      val oos = new ObjectOutputStream(new ByteArrayOutputStream())
      oos.writeObject(cass) must throwA[IOException]
    }
  }

  protected def javaTime(year: Int, month: Int, day: Int, hour: Int, minute: Int) =
    java.sql.Timestamp.valueOf(s"$year-$month-$day $hour:$minute:00")
  protected def javaBD(x: Double) = Decimal(x).toJavaBigDecimal
  protected def javaBD(str: String) = Decimal(str).toJavaBigDecimal
}