package com.ignition.workflow.grid

import java.io.File

import scala.xml.PCData

import org.joda.time.{ DateTime, DateTimeZone }
import org.junit.runner.RunWith
import org.specs2.matcher.XmlMatchers
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import com.eaio.uuid.UUID
import com.ignition.SparkTestHelper
import com.ignition.data.{ columnInfo2metaData, datetime, decimal, double, int, uuid }
import com.ignition.workflow.rdd.grid.input.DataGridInput
import com.ignition.workflow.rdd.grid.output.TextFileOutput

@RunWith(classOf[JUnitRunner])
class TextFileOutputSpec extends Specification with XmlMatchers with SparkTestHelper {

  val file = File.createTempFile("ign", ".csv")
  file.deleteOnExit

  val meta = uuid("customer_id") ~ datetime("date") ~ decimal("total") ~ int("items") ~ double("weight")
  val id = new UUID
  val date = (new DateTime(2015, 3, 1, 0, 0)).withZone(DateTimeZone.UTC)

  "CsvOutput" should {
    "create output file" in {
      val grid = DataGridInput(meta)
        .addRow(id, date.withHourOfDay(10), BigDecimal(123.45), 3, 9.23)
        .addRow(id, date.withHourOfDay(11), BigDecimal(650.0), 1, 239.0)
      val csv = TextFileOutput(file.getPath, "date" -> "%s", "total" -> "%7.2f",
        "items" -> "%3d", "weight" -> "%7.1f")
      grid.connectTo(csv).output
      scala.io.Source.fromFile(file).getLines.size === 3
    }
    "save to xml" in {
      val csv = TextFileOutput(file.getPath, "date" -> "%s", "total" -> "%7.2f",
        "items" -> "%3d", "weight" -> "%7.1f")
      <textfile-output header="true">
        <file>{ PCData(file.getPath) }</file>
        <separator>{ PCData(",") }</separator>
        <fields>
          <field name="date">%s</field>
          <field name="total">%7.2f</field>
          <field name="items">%3d</field>
          <field name="weight">%7.1f</field>
        </fields>
      </textfile-output> must ==/(csv.toXml)
    }
    "load from xml" in {
      val xml = <textfile-output header="true">
                  <file>{ PCData(file.getPath) }</file>
                  <separator>{ PCData(",") }</separator>
                  <fields>
                    <field name="date">%s</field>
                    <field name="total">%7.2f</field>
                    <field name="items">%3d</field>
                    <field name="weight">%7.1f</field>
                  </fields>
                </textfile-output>
      TextFileOutput.fromXml(xml) === TextFileOutput(file.getPath, "date" -> "%s", "total" -> "%7.2f",
        "items" -> "%3d", "weight" -> "%7.1f")
    }
  }
}