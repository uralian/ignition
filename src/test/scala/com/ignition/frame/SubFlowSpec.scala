package com.ignition.frame

import org.apache.spark.sql.types.Decimal
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.RichProduct
import com.ignition.types.{ RichStructType, boolean, date, decimal, double, fieldToRichStruct, string }

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

  val query = SQLQuery("""
            SELECT o.order_date, o.amount, c.name, c.cost, o.amount + c.cost AS total
            FROM input0 c
            JOIN input1 o
            ON o.name = c.name
            ORDER BY c.name, total""")

  "SubFlow" should {
    "work for no inputs and multiple outputs" in {
      val flow = SubFlow(3) { output =>
        val g1 = DataGrid(customerSchema).addRow("jill", true, 12.34)
        val g2 = DataGrid(orderSchema).addRow(javaDate(2015, 3, 3), Decimal(0.99), "jill")
        (g1, g2) --> query
        (g1, g2, query) --> output
      }
      assertOutput(flow, 0, ("jill", true, 12.34))
      assertOutput(flow, 1, (javaDate(2015, 3, 3), javaBD(0.99), "jill"))
      assertOutput(flow, 2, (javaDate(2015, 3, 3), javaBD(0.99), "jill", 12.34, javaBD(13.33)))
    }
    "work for multiple inputs and outputs" in {
      val flow = SubFlow(2, 3) { (input, output) =>
        input.out(0) --> (query.in(0), output.in(0))
        input.out(1) --> (query.in(1), output.in(1))
        query --> output.in(2)
      }
      (customerGrid, orderGrid) --> flow

      flow.output(0).collect.toSet == customerGrid.output.collect.toSet
      assertSchema(customerSchema, flow, 0)

      flow.output(1).collect.toSet == orderGrid.output.collect.toSet
      assertSchema(orderSchema, flow, 1)

      assertOutput(flow, 2,
        Seq(javaDate(2010, 3, 10), javaBD(42.85), "jack", 74.15, javaBD("117.00")),
        Seq(javaDate(2010, 1, 3), javaBD(120.55), "john", 25.36, javaBD(145.91)),
        Seq(javaDate(2010, 2, 9), javaBD(44.17), "john", 25.36, javaBD(69.53)),
        Seq(javaDate(2010, 1, 3), javaBD(55.08), "john", 25.36, javaBD(80.44)),
        Seq(javaDate(2010, 5, 10), javaBD(66.99), "jane", 19.99, javaBD(86.98)))
      assertSchema(orderSchema ~ double("cost") ~ decimal("total"), flow, 2)
    }
    "save to/load from xml" in {
      val flow = SubFlow(2, 4) { (input, output) =>
        input.out(0) --> (query.in(0), output.in(0))
        customerGrid --> query.in(1)
        customerGrid --> output.in(1)
        input.out(1) --> Filter("abc < 3") --> output.in(2)

        query --> output.in(3)
      }
      val xml = flow.toXml
      (xml \ "connections") must ==/(
        <connections>
          <connect src="INPUT" srcPort="1" tgt="filter0" tgtPort="0"/>
          <connect src="INPUT" srcPort="0" tgt="sql0" tgtPort="0"/>
          <connect src="datagrid0" srcPort="0" tgt="sql0" tgtPort="1"/>
          <connect src="INPUT" srcPort="0" tgt="OUTPUT" tgtPort="0"/>
          <connect src="datagrid0" srcPort="0" tgt="OUTPUT" tgtPort="1"/>
          <connect src="filter0" srcPort="0" tgt="OUTPUT" tgtPort="2"/>
          <connect src="sql0" srcPort="0" tgt="OUTPUT" tgtPort="3"/>
        </connections>)

      (xml \ "steps" \ DataGrid.tag).headOption must beSome
      (xml \ "steps" \ Filter.tag).headOption must beSome
      (xml \ "steps" \ SQLQuery.tag).headOption must beSome

      SubFlow.fromXml(xml) === flow
    }
    "save to/load from json" in {
      import org.json4s._
      import org.json4s.JsonDSL._
      import com.ignition.util.JsonUtils._

      val flow = SubFlow(2, 4) { (input, output) =>
        input.out(0) --> (query.in(0), output.in(0))
        customerGrid --> query.in(1)
        customerGrid --> output.in(1)
        input.out(1) --> Filter("abc < 3") --> output.in(2)

        query --> output.in(3)
      }

      val json = flow.toJson
      (json \ "connections" asArray).toSet === Set(
        ("src" -> "INPUT") ~ ("srcPort" -> 1) ~ ("tgt" -> "filter0") ~ ("tgtPort" -> 0),
        ("src" -> "INPUT") ~ ("srcPort" -> 0) ~ ("tgt" -> "sql0") ~ ("tgtPort" -> 0),
        ("src" -> "datagrid0") ~ ("srcPort" -> 0) ~ ("tgt" -> "sql0") ~ ("tgtPort" -> 1),
        ("src" -> "INPUT") ~ ("srcPort" -> 0) ~ ("tgt" -> "OUTPUT") ~ ("tgtPort" -> 0),
        ("src" -> "datagrid0") ~ ("srcPort" -> 0) ~ ("tgt" -> "OUTPUT") ~ ("tgtPort" -> 1),
        ("src" -> "filter0") ~ ("srcPort" -> 0) ~ ("tgt" -> "OUTPUT") ~ ("tgtPort" -> 2),
        ("src" -> "sql0") ~ ("srcPort" -> 0) ~ ("tgt" -> "OUTPUT") ~ ("tgtPort" -> 3))

      ((json \ "steps" \ "tag" asArray) map (_ asString) toSet) === Set("datagrid", "filter", "sql")
    }
    "be unserializable" in assertUnserializable(SubFlow(2, 4)((input, output) => {}))
  }
}