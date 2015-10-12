package com.ignition.frame

import scala.math.BigInt.int2bigInt

import org.apache.spark.sql.Row
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.types.{ RichStructType, fieldToRichStruct, int, string }

@RunWith(classOf[JUnitRunner])
class DataGridSpec extends FrameFlowSpecification {

  lazy val xml =
    <datagrid>
      <schema>
        <field name="id" type="string" nullable="false"/>
        <field name="label" type="string" nullable="true"/>
        <field name="index" type="integer" nullable="true"/>
      </schema>
      <rows>
        <row>
          <item>3815cb50-ccca-11e4-80dc-027f371e36df</item>
          <item>ABC</item>
          <item>52</item>
        </row>
        <row>
          <item>3815cb50-ccca-11e4-80dc-027f371e36df</item>
          <item/>
          <item>3</item>
        </row>
        <row>
          <item>a3fa87b1-ad63-11e4-9d3b-0a0027000000</item>
          <item>XYZ</item>
          <item/>
        </row>
      </rows>
    </datagrid>

  "DataGrid" should {
    "produce data frame" in {
      val grid = DataGrid(string("name") ~ int("age"))
        .addRow("john", 25).addRow("jane", 33).addRow("jack", 51)
      assertSchema(string("name") ~ int("age"), grid, 0)
      assertOutput(grid, 0, Seq("john", 25), Seq("jane", 33), Seq("jack", 51))
    }
    "save to/load from xml" in {
      val grid = DataGrid.fromXml(xml)

      grid.schema === string("id", false) ~ string("label") ~ int("index")
      grid.rows.size === 3
      grid.rows(0).toSeq === Seq("3815cb50-ccca-11e4-80dc-027f371e36df", "ABC", 52)
      grid.rows(1).toSeq === Seq("3815cb50-ccca-11e4-80dc-027f371e36df", null, 3)
      grid.rows(2).toSeq === Seq("a3fa87b1-ad63-11e4-9d3b-0a0027000000", "XYZ", null)

      DataGrid.fromXml(grid.toXml) === grid
    }
    "save to/load from json" in {
      import org.json4s._
      import org.json4s.JsonDSL._

      val grid = DataGrid.fromXml(xml)
      grid.toJson === ("tag" -> "datagrid") ~ ("schema" -> List(
        ("name" -> "id") ~ ("type" -> "string") ~ ("nullable" -> false),
        ("name" -> "label") ~ ("type" -> "string") ~ ("nullable" -> true),
        ("name" -> "index") ~ ("type" -> "integer") ~ ("nullable" -> true))) ~
        ("rows" -> List(
          List(JString("3815cb50-ccca-11e4-80dc-027f371e36df"), JString("ABC"), JInt(52)),
          List(JString("3815cb50-ccca-11e4-80dc-027f371e36df"), null, JInt(3)),
          List(JString("a3fa87b1-ad63-11e4-9d3b-0a0027000000"), JString("XYZ"), null)))
      DataGrid.fromJson(grid.toJson) === grid
    }
    "produce valid DataFrame" in {
      val grid = DataGrid.fromXml(xml)
      assertOutput(grid, 0,
        Seq("3815cb50-ccca-11e4-80dc-027f371e36df", "ABC", 52),
        Seq("3815cb50-ccca-11e4-80dc-027f371e36df", null, 3),
        Seq("a3fa87b1-ad63-11e4-9d3b-0a0027000000", "XYZ", null))
    }
    "fail for wrong column count" in {
      DataGrid(string("a") ~ int("b")).addRow("abc") must throwA[AssertionError]
      DataGrid(string("a") ~ int("b")).addRow("a", 5, true) must throwA[AssertionError]
    }
    "fail for null in a non-nullable column" in {
      DataGrid(string("a", false) ~ int("b")).addRow(null, 5) must throwA[AssertionError]
    }
    "fail for wrong column type" in {
      DataGrid(string("a") ~ int("b")).addRow(2, 5) must throwA[AssertionError]
      DataGrid(string("a") ~ int("b")).addRow("a", true) must throwA[AssertionError]
      DataGrid(string("a") ~ int("b")).addRow("a", 5.5) must throwA[AssertionError]
    }
    "be unserializable" in assertUnserializable(DataGrid.fromXml(xml))
  }
}