package com.ignition.workflow.grid

import org.junit.runner.RunWith
import org.specs2.matcher.XmlMatchers
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import com.eaio.uuid.UUID
import com.ignition.SparkTestHelper
import com.ignition.data.ColumnInfo
import com.ignition.data.DataType.{ IntDataType, StringDataType, UUIDDataType }
import com.ignition.workflow.rdd.grid.input.DataGridInput
import com.ignition.data.DefaultDataRow

@RunWith(classOf[JUnitRunner])
class DataGridInputSpec extends Specification with XmlMatchers with SparkTestHelper {
  sequential

  lazy val xml =
    <grid-input>
      <meta>
        <col name="id" type="uuid"/>
        <col name="label" type="string"/>
        <col name="index" type="int"/>
      </meta>
      <rows>
        <row>
          <item>d2aa8254-343f-46e2-8a64-7bffad2478de</item>
          <item>ABC</item>
          <item>52</item>
        </row>
        <row>
          <item>a3fa87b1-ad63-11e4-9d3b-0a0027000000</item>
          <item>XYZ</item>
          <item/>
        </row>
      </rows>
    </grid-input>

  "DataGridInput" should {
    "load from xml" in {
      val grid = DataGridInput.fromXml(xml)
      grid.meta.columns === Vector(ColumnInfo[UUID]("id"), ColumnInfo[String]("label"), ColumnInfo[Int]("index"))
      grid.rows.size === 2
      grid.rows(0).rawData === Vector(new UUID("d2aa8254-343f-46e2-8a64-7bffad2478de"), "ABC", 52)
      grid.rows(1).rawData === Vector(new UUID("a3fa87b1-ad63-11e4-9d3b-0a0027000000"), "XYZ", null)
    }
    "yield correct metadata" in {
      val grid = DataGridInput.fromXml(xml)
      grid.outMetaData === Some(grid.meta)
    }
    "save to xml" in {
      val grid = DataGridInput.fromXml(xml)
      val xml2 = grid.toXml
      xml must ==/(xml2)
    }
    "produce valid metadata" in {
      val grid = DataGridInput.fromXml(xml)
      grid.outMetaData === Some(grid.meta)
    }
    "produce valid RDD" in {
      val grid = DataGridInput.fromXml(xml)
      val rdd = grid.output
      rdd.count === 2
      val columnNames = Vector("id", "label", "index")
      val row0 = Vector(new UUID("d2aa8254-343f-46e2-8a64-7bffad2478de"), "ABC", 52)
      val row1 = Vector(new UUID("a3fa87b1-ad63-11e4-9d3b-0a0027000000"), "XYZ", null)
      rdd.collect.toSet === Set(DefaultDataRow(columnNames, row0), DefaultDataRow(columnNames, row1))
    }
  }
}