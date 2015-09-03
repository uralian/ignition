package com.ignition.frame

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.CassandraSpec
import com.ignition.types._

@RunWith(classOf[JUnitRunner])
class CassandraInputSpec extends FrameFlowSpecification with CassandraSpec {
  sequential

  val keySpace = "ignition"
  val dataSet = "ignition_test.ddl"

  override def afterAll = {
    super[CassandraSpec].afterAll
    super[FrameFlowSpecification].afterAll
  }

  "CassandraInput" should {
    "handle all C* data types" in {
      // skipping BLOB because of the bug in Spec2 array comparison
      val fields = List("rowkey", "cellkey", "x_ascii", "x_bigint", "x_boolean", "x_decimal",
        "x_double", "x_float", "x_inet", "x_int", "x_text", "x_timestamp", "x_timeuuid",
        "x_uuid", "x_varchar", "x_varint")
      val cass = CassandraInput("ignition", "all_types", fields)

      val schema = int("rowkey", false) ~ timestamp("cellkey", false) ~ string("x_ascii") ~
        long("x_bigint") ~ boolean("x_boolean") ~ decimal("x_decimal") ~ double("x_double") ~
        float("x_float") ~ string("x_inet") ~ int("x_int") ~ string("x_text") ~ timestamp("x_timestamp") ~
        string("x_timeuuid") ~ string("x_uuid") ~ string("x_varchar") ~ decimal("x_varint")
      assertSchema(schema, cass)

      assertOutput(cass, 0,
        Seq(1, javaTime(2015, 9, 1, 0, 0), "blah", 12345L, true, javaBD("123.45"),
          123.45, 123.45f, null, null, null, null, null, null, null, null),
        Seq(1, javaTime(2015, 9, 2, 0, 0), "blah", 12345L, true, javaBD("123"),
          123.0, 123.0f, null, null, null, null, null, null, null, null),
        Seq(2, javaTime(2015, 9, 3, 0, 0), null, null, null, null, null, null,
          "/127.0.0.1", 123, "blah", javaTime(2015, 9, 2, 15, 30), "fa1ebd08-51d4-11e5-885d-feff819cdc9f",
          "de305d54-75b4-431b-adb2-eb6b9e546014", "blah", javaBD("12345")))

      // testing BLOB separately
      val cass2 = CassandraInput("ignition", "all_types") % ("rowkey", "x_blob")
      val out2 = cass2.output(0).collect.toSet
      val blob = Array(98.toByte, 108.toByte, 97.toByte, 104.toByte)
      out2 foreach { row =>
        val key = row(0)
        val data = row(1)
        if (key == 1) data === blob else data must beNull
      }
      success
    }
    "load data without select or filtering" in {
      val cass = CassandraInput("ignition", "orders")

      val schema = string("customer_id", false) ~ timestamp("date", false) ~ string("description") ~
        int("items") ~ boolean("shipped") ~ decimal("total") ~ double("weight")
      assertSchema(schema, cass)

      val output = cass.output
      output.count === 8
      val row = output.filter("customer_id = 'c7b44cb2-b6bf-11e4-a71e-12e3f512a338'").first
      row(0) === "c7b44cb2-b6bf-11e4-a71e-12e3f512a338"
      row(1) === javaTime(2015, 1, 5, 0, 0)
      row(2) === "furniture"
      row(3) === 1
      row(4) === true
      row(5) === javaBD("620.00")
      row(6) === 650.0
    }
    "load data without filtering" in {
      val cass = CassandraInput("ignition", "orders") % ("customer_id", "total")

      assertSchema(string("customer_id", false) ~ decimal("total"), cass)

      val output = cass.output
      output.count === 8
      val row = output.filter("customer_id = 'c7b44cb2-b6bf-11e4-a71e-12e3f512a338'").first
      row(0) === "c7b44cb2-b6bf-11e4-a71e-12e3f512a338"
      row(1) === javaBD("620.00")
    }
    "filter data by partitioning columns" in {
      val input = CassandraInput("ignition", "orders") where ("customer_id=?",
        "d3fdcf34-b6bf-11e4-a71e-12e3f512a338")
      input.output.count === 2
    }
    "filter data by partitioning and clustering columns" in {
      val input = CassandraInput("ignition", "orders") where ("customer_id=? and date>=? and date<=?",
        "c7b44500-b6bf-11e4-a71e-12e3f512a338",
        javaDate(2015, 1, 8), javaDate(2015, 1, 20))
      input.output.count === 1
    }
    "filter data by partitioning, clustering, and indexed columns" in {
      val input = CassandraInput("ignition", "orders") where ("customer_id=? and date>=? and date<=? and description=?",
        "c7b44500-b6bf-11e4-a71e-12e3f512a338", javaDate(2015, 1, 1), javaDate(2015, 3, 1), "clothes")
      input.output.count === 2
    }
    "preview a subset of rows" in {
      val cass = CassandraInput("ignition", "orders")
      cass.output(Some(5)).count === 5
    }
    "save to xml/load from xml" in {
      val c1 = CassandraInput("ignition", "orders")
      c1.toXml must ==/(<cassandra-input keyspace="ignition" table="orders"/>)
      CassandraInput.fromXml(c1.toXml) === c1

      val c2 = CassandraInput("ignition", "orders") columns ("items", "total")
      c2.toXml must ==/(
        <cassandra-input keyspace="ignition" table="orders">
          <columns>
            <column name="items"/>
            <column name="total"/>
          </columns>
        </cassandra-input>)
      CassandraInput.fromXml(c2.toXml) === c2

      val c3 = CassandraInput("ignition", "orders") columns ("items", "total") where (
        "customer_id=? and date>=? and date<=? and description=?",
        "c7b44500-b6bf-11e4-a71e-12e3f512a338", javaDate(2015, 1, 1), javaDate(2015, 3, 1), "clothes")
      c3.toXml must ==/(
        <cassandra-input keyspace="ignition" table="orders">
          <columns>
            <column name="items"/>
            <column name="total"/>
          </columns>
          <where>
            <cql>{ "customer_id=? and date>=? and date<=? and description=?" }</cql>
            <arg type="string">c7b44500-b6bf-11e4-a71e-12e3f512a338</arg>
            <arg type="date">2015-01-01</arg>
            <arg type="date">2015-03-01</arg>
            <arg type="string">clothes</arg>
          </where>
        </cassandra-input>)
      CassandraInput.fromXml(c3.toXml) === c3
    }
    "save to xml/load from json" in {
      import org.json4s.JsonDSL._

      val c1 = CassandraInput("ignition", "orders")
      c1.toJson === ("tag" -> "cassandra-input") ~ ("keyspace" -> "ignition") ~ ("table" -> "orders") ~
        ("columns" -> Option.empty[String]) ~ ("where" -> Option.empty[String])
      CassandraInput.fromJson(c1.toJson) === c1

      val c2 = CassandraInput("ignition", "orders") columns ("items", "total")
      c2.toJson === ("tag" -> "cassandra-input") ~ ("keyspace" -> "ignition") ~ ("table" -> "orders") ~
        ("columns" -> List("items", "total")) ~ ("where" -> Option.empty[String])
      CassandraInput.fromJson(c2.toJson) === c2

      val c3 = CassandraInput("ignition", "orders") columns ("items", "total") where (
        "customer_id=? and date>=? and date<=? and description=?",
        "c7b44500-b6bf-11e4-a71e-12e3f512a338", javaDate(2015, 1, 1), javaDate(2015, 3, 1), "clothes")
      c3.toJson === ("tag" -> "cassandra-input") ~ ("keyspace" -> "ignition") ~ ("table" -> "orders") ~
        ("columns" -> List("items", "total")) ~
        ("where" -> ("cql" -> "customer_id=? and date>=? and date<=? and description=?") ~
          ("args" -> List(
            ("type" -> "string") ~ ("value" -> "c7b44500-b6bf-11e4-a71e-12e3f512a338"),
            ("type" -> "date") ~ ("value" -> "2015-01-01"),
            ("type" -> "date") ~ ("value" -> "2015-03-01"),
            ("type" -> "string") ~ ("value" -> "clothes"))))
      CassandraInput.fromJson(c3.toJson) === c3
    }
    "be unserializable" in assertUnserializable(CassandraInput("ignition", "orders"))
  }
}