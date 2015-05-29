package com.ignition.frame

import org.apache.spark.sql.types.Decimal
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.types._

@RunWith(classOf[JUnitRunner])
class SubFlowSpec extends FrameFlowSpecification {
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

  val fi = FlowInput(customerSchema, orderSchema)
  val fo = FlowOutput(3)
  fo.connectFrom(0, fi, 1)
  fo.connectFrom(1, fi, 0)
  val query = SQLQuery("""
          SELECT o.order_date, o.amount, c.name, c.cost, o.amount + c.cost AS total
          FROM input0 c
          JOIN input1 o
          ON o.name = c.name
          ORDER BY c.name, total""")
  query.connectFrom(0, fi, 0)
  query.connectFrom(1, fi, 1)
  fo.connectFrom(2, query, 0)

  "SubFlow" should {
    "be testable in isolation" in {
      val testCustomerGrid = DataGrid(customerSchema).addRow("jill", true, 12.34)
      val testOrderGrid = DataGrid(orderSchema).addRow(javaDate(2015, 3, 3), Decimal(0.99), "jill")
      fi.connectFrom(0, testCustomerGrid, 0)
      fi.connectFrom(1, testOrderGrid, 0)
      fo.outputCount === 3
      assertOutput(fo, 0, Seq(javaDate(2015, 3, 3), javaBD(0.99), "jill"))
      assertOutput(fo, 1, Seq("jill", true, 12.34))
      assertOutput(fo, 2, Seq(javaDate(2015, 3, 3), javaBD(0.99), "jill", 12.34, javaBD(13.33)))
    }
    "work when connected from outside" in {
      val flow = SubFlow(fi, fo)
      flow.connectFrom(0, customerGrid, 0)
      flow.connectFrom(1, orderGrid, 0)
      assertOutput(flow, 2,
        Seq(javaDate(2010, 3, 10), javaBD(42.85), "jack", 74.15, javaBD("117.00")),
        Seq(javaDate(2010, 1, 3), javaBD(120.55), "john", 25.36, javaBD(145.91)),
        Seq(javaDate(2010, 2, 9), javaBD(44.17), "john", 25.36, javaBD(69.53)),
        Seq(javaDate(2010, 1, 3), javaBD(55.08), "john", 25.36, javaBD(80.44)),
        Seq(javaDate(2010, 5, 10), javaBD(66.99), "jane", 19.99, javaBD(86.98)))
      assertSchema(orderSchema ~ double("cost") ~ decimal("total"), flow, 2)
      assertSchema(orderSchema, flow, 0)
      assertSchema(customerSchema, flow, 1)
    }
    "be unserializable" in assertUnserializable(SQLQuery("SELECT * FROM input0"))
  }
}