package com.ignition.flow

import org.apache.spark.sql.types.Decimal
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import com.ignition.types._
import com.ignition.ExecutionException

@RunWith(classOf[JUnitRunner])
class SQLQuerySpec extends FlowSpecification {
  sequential

  val customerSchema = string("name") ~ boolean("local") ~ double("cost")
  val orderSchema = date("order_date") ~ decimal("amount") ~ string("name")

  val customerGrid = DataGrid(customerSchema)
    .addRow("john", true, 25.36)
    .addRow("jack", false, 74.15)
    .addRow("jane", true, 19.99)

  val orderGrid = DataGrid(orderSchema)
    .addRow(javaDate(2010, 1, 3), Decimal(120.55), "john")
    .addRow(javaDate(2010, 3, 10), Decimal(42.85), "jack")
    .addRow(javaDate(2010, 2, 9), Decimal(44.17), "john")
    .addRow(javaDate(2010, 5, 10), Decimal(66.99), "jane")
    .addRow(javaDate(2010, 1, 3), Decimal(55.08), "john")

  "SQLQuery" should {
    "yield result from one source" in {
      val query = SQLQuery("SELECT order_date, SUM(amount) AS total FROM input0 GROUP BY order_date")
      orderGrid --> 0 :| query
      assertOutput(query, 0,
        Seq(javaDate(2010, 1, 3), javaBD(175.63)),
        Seq(javaDate(2010, 2, 9), javaBD(44.17)),
        Seq(javaDate(2010, 3, 10), javaBD(42.85)),
        Seq(javaDate(2010, 5, 10), javaBD(66.99)))
    }
    "yield result from two joined sources" in {
      val query = SQLQuery("""
          SELECT o.order_date, o.amount, c.name, c.cost, o.amount + c.cost AS total
          FROM input0 c
          JOIN input1 o
          ON o.name = c.name
          ORDER BY c.name, total""")
      (customerGrid, orderGrid) --> query
      assertOutput(query, 0,
        Seq(javaDate(2010, 3, 10), javaBD(42.85), "jack", 74.15, javaBD("117.00")),
        Seq(javaDate(2010, 1, 3), javaBD(120.55), "john", 25.36, javaBD(145.91)),
        Seq(javaDate(2010, 2, 9), javaBD(44.17), "john", 25.36, javaBD(69.53)),
        Seq(javaDate(2010, 1, 3), javaBD(55.08), "john", 25.36, javaBD(80.44)),
        Seq(javaDate(2010, 5, 10), javaBD(66.99), "jane", 19.99, javaBD(86.98)))
      assertSchema(orderSchema ~ double("cost") ~ decimal("total"), query)
    }
    "fail on disconnected inputs" in {
      val query = SQLQuery("SELECT * FROM input0")
      query.output must throwA[ExecutionException]
    }
    "save to xml" in {
      val query = SQLQuery("SELECT * FROM input0 WHERE amount > 5")
      <sql>SELECT * FROM input0 WHERE amount > 5</sql> must ==/(query.toXml)
    }
    "load from xml" in {
      SQLQuery.fromXml(<sql>SELECT * FROM input0 WHERE amount > 5</sql>) ===
        SQLQuery("SELECT * FROM input0 WHERE amount > 5")
    }
    "be unserializable" in assertUnserializable(SQLQuery("SELECT * FROM input0"))
  }
}