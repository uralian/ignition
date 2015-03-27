package com.ignition.flow

import java.io.{ ByteArrayOutputStream, IOException, ObjectOutputStream }

import org.apache.spark.sql.types.Decimal
import org.junit.runner.RunWith
import org.specs2.matcher.XmlMatchers
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import com.ignition.SparkTestHelper
import com.ignition.types._

@RunWith(classOf[JUnitRunner])
class SubFlowSpec extends Specification with XmlMatchers with SparkTestHelper {
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

  val fi = FlowInput(Array(customerSchema, orderSchema))
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
      fo.output(0).collect.map(_.toSeq) === Array(Seq(javaDate(2015, 3, 3), javaBD(0.99), "jill"))
      fo.output(1).collect.map(_.toSeq) === Array(Seq("jill", true, 12.34))
      fo.output(2).collect.map(_.toSeq) === Array(Seq(javaDate(2015, 3, 3), javaBD(0.99), "jill", 12.34, javaBD(13.33)))
    }
    "work when connected from outside" in {
      val flow = SubFlow(fi, fo)
      flow.connectFrom(0, customerGrid, 0)
      flow.connectFrom(1, orderGrid, 0)
      flow.output(2).collect.map(_.toSeq).toSet === Set(
        Seq(javaDate(2010, 3, 10), javaBD(42.85), "jack", 74.15, javaBD("117.00")),
        Seq(javaDate(2010, 1, 3), javaBD(120.55), "john", 25.36, javaBD(145.91)),
        Seq(javaDate(2010, 2, 9), javaBD(44.17), "john", 25.36, javaBD(69.53)),
        Seq(javaDate(2010, 1, 3), javaBD(55.08), "john", 25.36, javaBD(80.44)),
        Seq(javaDate(2010, 5, 10), javaBD(66.99), "jane", 19.99, javaBD(86.98)))
      flow.outputSchema(2) === Some(orderSchema ~ double("cost") ~ decimal("total"))
      flow.outputSchema(0) === Some(orderSchema)
      flow.outputSchema(1) === Some(customerSchema)
    }
    "be unserializable" in {
      val query = SQLQuery("SELECT * FROM input0")
      val oos = new ObjectOutputStream(new ByteArrayOutputStream())
      oos.writeObject(query) must throwA[IOException]
    }
  }

  protected def javaDate(year: Int, month: Int, day: Int) = java.sql.Date.valueOf(s"$year-$month-$day")
  protected def javaBD(x: Double) = Decimal(x).toJavaBigDecimal
  protected def javaBD(str: String) = Decimal(str).toJavaBigDecimal
}