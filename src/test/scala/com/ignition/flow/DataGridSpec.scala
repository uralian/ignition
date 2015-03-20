package com.ignition.flow

import java.io.{ ByteArrayOutputStream, IOException, ObjectOutputStream }

import org.junit.runner.RunWith
import org.specs2.matcher.XmlMatchers
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import com.ignition.SparkTestHelper
import com.ignition.types.{ RichStructType, fieldToStruct, int, string }

@RunWith(classOf[JUnitRunner])
class DataGridSpec extends Specification with XmlMatchers with SparkTestHelper {

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
    "load from xml" in {
      val grid = DataGrid.fromXml(xml)
      grid.schema === string("id", false) ~ string("label") ~ int("index")
      grid.rows.size === 3
      grid.rows(0).toSeq === Seq("3815cb50-ccca-11e4-80dc-027f371e36df", "ABC", 52)
      grid.rows(1).toSeq === Seq("3815cb50-ccca-11e4-80dc-027f371e36df", null, 3)
      grid.rows(2).toSeq === Seq("a3fa87b1-ad63-11e4-9d3b-0a0027000000", "XYZ", null)
    }
    "save to xml" in {
      val grid = DataGrid.fromXml(xml)
      val xml2 = grid.toXml
      xml must ==/(xml2)
    }
    "yield correct metadata" in {
      val grid = DataGrid.fromXml(xml)
      grid.outputSchema === Some(grid.schema)
    }
    "produce valid DataFrame" in {
      val grid = DataGrid.fromXml(xml)
      val df = grid.output
      df.count === 3
      df.collect.map(_.toSeq).toSet === Set(
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
    "be unserializable" in {
      val grid = DataGrid.fromXml(xml)
      val oos = new ObjectOutputStream(new ByteArrayOutputStream())
      oos.writeObject(grid) must throwA[IOException]
    }
  }
}