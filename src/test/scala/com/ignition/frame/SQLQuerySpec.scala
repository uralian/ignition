package com.ignition.frame

import org.apache.spark.sql.types.Decimal
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.ExecutionException
import com.ignition.types._

@RunWith(classOf[JUnitRunner])
class SQLQuerySpec extends FrameFlowSpecification {
  sequential

  val customerSchema = string("name") ~ boolean("local") ~ double("cost")
  val orderSchema = date("order_date") ~ decimal("amount") ~ string("name")

  val customerGrid = DataGrid(customerSchema)
    .addRow("john", true, 25.36)
    .addRow("jack", false, 74.15)
    .addRow("jane", true, 19.25)

  val orderGrid = DataGrid(orderSchema)
    .addRow(javaDate(2010, 1, 3), Decimal(120.55), "john")
    .addRow(javaDate(2010, 3, 10), Decimal(42.85), "jack")
    .addRow(javaDate(2010, 2, 9), Decimal(44.17), "john")
    .addRow(javaDate(2010, 5, 10), Decimal(66.0), "jane")
    .addRow(javaDate(2010, 1, 3), Decimal(55.08), "john")

  "SQLQuery" should {
    "yield result from one source" in {
      val query = SQLQuery("SELECT order_date, SUM(amount) AS total FROM input0 GROUP BY order_date")
      orderGrid --> query.in(0)
      assertOutput(query, 0,
        Seq(javaDate(2010, 1, 3), javaBD(175.63)),
        Seq(javaDate(2010, 2, 9), javaBD(44.17)),
        Seq(javaDate(2010, 3, 10), javaBD(42.85)),
        Seq(javaDate(2010, 5, 10), javaBD(66.0)))
    }
    "yield result from two joined sources" in {
      val query = SQLQuery("""
          SELECT o.order_date, o.amount, c.name, c.cost, o.amount + c.cost AS total
          FROM input0 c
          JOIN input1 o
          ON o.name = c.name
          ORDER BY c.name, total""")
      (customerGrid, orderGrid) --> query
      val df = query.output(0, false)
      df.count === 5
      df.collect.toSet.filter(_.getDate(0) == javaDate(2010, 1, 3)) ===
        Set(
          anySeqToRow(Seq(javaDate(2010, 1, 3), javaBD(120.55), "john", 25.36, 145.91)),
          anySeqToRow(Seq(javaDate(2010, 1, 3), javaBD(55.08), "john", 25.36, 80.44)))
      assertSchema(orderSchema ~ double("cost") ~ double("total"), query)
    }
    "fail on disconnected inputs" in {
      val query = SQLQuery("SELECT * FROM input0")
      query.output must throwA[ExecutionException]
    }
    "save to/load from xml" in {
      val query = SQLQuery("SELECT * FROM input0 WHERE amount > 5")
      query.toXml must ==/(<sql>SELECT * FROM input0 WHERE amount > 5</sql>)
      SQLQuery.fromXml(query.toXml) === query
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._

      val query = SQLQuery("SELECT * FROM input0 WHERE amount > 5")
      query.toJson === ("tag" -> "sql") ~ ("query" -> "SELECT * FROM input0 WHERE amount > 5")
      SQLQuery.fromJson(query.toJson) === query
    }
    "be unserializable" in assertUnserializable(SQLQuery("SELECT * FROM input0"))
  }
}