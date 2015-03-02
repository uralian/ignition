package com.ignition.workflow.grid

import org.joda.time.{ DateTime, DateTimeZone }
import org.junit.runner.RunWith
import org.specs2.matcher.XmlMatchers
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import com.eaio.uuid.UUID
import com.ignition.{ CassandraSpec, SparkTestHelper }
import com.ignition.data.{ columnInfo2metaData, datetime, decimal, double, int, uuid }
import com.ignition.workflow.rdd.grid.input.DataGridInput
import com.ignition.workflow.rdd.grid.output.CassandraOutput

@RunWith(classOf[JUnitRunner])
class CassandraOutputSpec extends Specification with XmlMatchers with CassandraSpec with SparkTestHelper {
  sequential

  val keySpace = "ignition"
  val dataSet = "ignition_test.ddl"

  override def afterAll = {
    super[CassandraSpec].afterAll
    super[SparkTestHelper].afterAll
  }

  val meta = uuid("customer_id") ~ datetime("date") ~ decimal("total") ~ int("items") ~ double("weight")
  val id = new UUID
  val date = new DateTime(2015, 3, 1, 0, 0).withZone(DateTimeZone.UTC)

  "CassandraOutput" should {
    "save data to Cassandra" in {
      val grid = DataGridInput(meta)
        .addRow(id, date.withHourOfDay(10), BigDecimal(123.45), 3, 9.23)
        .addRow(id, date.withHourOfDay(11), BigDecimal(650.0), 1, 239.0)
      val cassOut = CassandraOutput(keySpace, "shipments")
      grid.connectTo(cassOut).output
      Thread.sleep(100)
      val query = s"select * from shipments where customer_id=${id} and date='2015-03-01 10:00-0000'"
      val row = session.execute(query).one
      (row == null) must beFalse
      row.getDecimal("total") === BigDecimal(123.45).bigDecimal
      row.getInt("items") === 3
      row.getDouble("weight") === 9.23
    }
    "save to xml" in {
      val cass = CassandraOutput("keyspace", "table")
      <cassandra-output keyspace="keyspace" table="table"/> must ==/(cass.toXml)
    }
    "load from xml" in {
      val xml = <cassandra-output keyspace="keyspace" table="table"/>
      CassandraOutput.fromXml(xml) === CassandraOutput("keyspace", "table")
    }
  }
}