package com.ignition.flow

import java.io.{ ByteArrayOutputStream, IOException, ObjectOutputStream }

import scala.Array.canBuildFrom

import org.apache.spark.sql.types.Decimal
import org.junit.runner.RunWith
import org.specs2.matcher.XmlMatchers
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import com.ignition.SparkTestHelper
import com.ignition.types._

@RunWith(classOf[JUnitRunner])
class SQLQuerySpec extends Specification with XmlMatchers with SparkTestHelper {
  import ctx.implicits._

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
      orderGrid -> query
      val output = query.output
      output.count === 4
      query.output.collect.map(_.toSeq).toSet === Set(
        Seq(javaDate(2010, 1, 3), Decimal(175.63).toJavaBigDecimal),
        Seq(javaDate(2010, 2, 9), Decimal(44.17).toJavaBigDecimal),
        Seq(javaDate(2010, 3, 10), Decimal(42.85).toJavaBigDecimal),
        Seq(javaDate(2010, 5, 10), Decimal(66.99).toJavaBigDecimal))
    }
    "yield result from two joined sources" in {
      val query = SQLQuery("""
          SELECT o.order_date, o.amount, c.name, c.cost, o.amount + c.cost AS total
          FROM input0 c
          JOIN input1 o
          ON o.name = c.name
          ORDER BY c.name, total""")
      (customerGrid, orderGrid) -> query
      query.output.show
      query.output.collect.map(_.toSeq).toSet === Set(
        Seq(javaDate(2010, 3, 10), javaBD(42.85), "jack", 74.15, javaBD("117.00")),
        Seq(javaDate(2010, 1, 3), javaBD(120.55), "john", 25.36, javaBD(145.91)),
        Seq(javaDate(2010, 2, 9), javaBD(44.17), "john", 25.36, javaBD(69.53)),
        Seq(javaDate(2010, 1, 3), javaBD(55.08), "john", 25.36, javaBD(80.44)),
        Seq(javaDate(2010, 5, 10), javaBD(66.99), "jane", 19.99, javaBD(86.98)))
      query.outputSchema === Some(orderSchema ~ double("cost") ~ decimal("total"))
    }
    "fail on disconnected inputs" in {
      val query = SQLQuery("SELECT * FROM input0")
      query.output must throwA[FlowExecutionException]
    }
    "save to xml" in {
      val query = SQLQuery("SELECT * FROM input0 WHERE amount > 5")
      <sql>SELECT * FROM input0 WHERE amount > 5</sql> must ==/(query.toXml)
    }
    "load from xml" in {
      SQLQuery.fromXml(<sql>SELECT * FROM input0 WHERE amount > 5</sql>) ===
        SQLQuery("SELECT * FROM input0 WHERE amount > 5")
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