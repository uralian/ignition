package com.ignition.frame

import org.apache.spark.sql.types.Decimal
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.eaio.uuid.UUID
import com.ignition.CassandraSpec
import com.ignition.types._

@RunWith(classOf[JUnitRunner])
class CassandraOutputSpec extends FrameFlowSpecification with CassandraSpec {
  sequential

  val keySpace = "ignition"
  val dataSet = "ignition_test.ddl"

  override def afterAll = {
    super[CassandraSpec].afterAll
    super[FrameFlowSpecification].afterAll
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
    }
    "save to xml" in {
      val cass = CassandraOutput("keyspace", "table")
      <cassandra-output keyspace="keyspace" table="table"/> must ==/(cass.toXml)
    }
    "load from xml" in {
      val xml = <cassandra-output keyspace="keyspace" table="table"/>
      CassandraOutput.fromXml(xml) === CassandraOutput("keyspace", "table")
    }
    "be unserializable" in assertUnserializable(CassandraInput("ignition", "orders", schema))
  }
}