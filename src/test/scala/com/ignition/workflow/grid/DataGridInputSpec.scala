package com.ignition.workflow.grid

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.TypeTag.Int

import org.apache.spark.SparkContext
import org.junit.runner.RunWith
import org.slf4j.LoggerFactory
import org.specs2.matcher.XmlMatchers
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import com.eaio.uuid.UUID
import com.ignition.BeforeAllAfterAll
import com.ignition.data.ColumnInfo
import com.ignition.data.DataType.{ IntDataType, StringDataType, UUIDDataType }
import com.ignition.data.DefaultDataRow
import com.ignition.workflow.rdd.grid.DataGridInput

@RunWith(classOf[JUnitRunner])
class DataGridInputSpec extends Specification with XmlMatchers with BeforeAllAfterAll {
  sequential

  val log = LoggerFactory.getLogger(getClass)

  implicit val sc = new SparkContext("local[4]", "test")

  override def afterAll() = {
    sc.stop
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.master.port")
  }

  lazy val xml =
    <grid>
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
    </grid>

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