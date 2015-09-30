package com.ignition.frame

import java.io.File

import scala.io.Source

import org.apache.spark.sql.types.Decimal
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.types._

@RunWith(classOf[JUnitRunner])
class TextFileOutputSpec extends FrameFlowSpecification {

  val schema = string("ssn") ~ string("name") ~ int("age") ~ double("weight") ~
    decimal("balance") ~ boolean("flag") ~ date("date") ~ timestamp("time")
  val grid = DataGrid(schema)
    .addRow("111-22-3333", "John", 25, 165.3, Decimal(1455.38), true, javaDate(2015, 1, 15), javaTime(2015, 4, 3, 12, 30))
    .addRow("222-33-4444", "Jane", 19, 124.0, Decimal(700.00), false, javaDate(2014, 10, 5), javaTime(2015, 2, 12, 9, 15))
    .addRow("333-00-1111", "Jake", 52, 192.4, Decimal(12150.50), false, javaDate(2015, 3, 21), javaTime(2015, 1, 30, 5, 10))

  "TextFileOutput" should {
    "work for string data" in {
      val file = createTestFile
      val csv = TextFileOutput(file.getPath) % ("ssn" -> "%12s", "name" -> "%-5s") % "name" separator "|" header false
      grid --> csv
      csv.output

      val lines = Source.fromFile(file).getLines
      lines.toSet === Set(
        " 111-22-3333|John |John",
        " 222-33-4444|Jane |Jane",
        " 333-00-1111|Jake |Jake")
    }
    "work for numeric data" in {
      val file = createTestFile
      val csv = TextFileOutput(file.getPath, "age" -> "%03d", "balance" -> "%10.2f", "weight" -> "%.3f")
      grid --> csv
      csv.output

      val lines = Source.fromFile(file).getLines
      lines.toSet === Set("age,balance,weight",
        "025,   1455.38,165.300",
        "019,    700.00,124.000",
        "052,  12150.50,192.400")
    }
    "work for time data" in {
      val file = createTestFile
      val csv = TextFileOutput(file.getPath, "date" -> "%s", "time" -> "%s").copy(separator = "|")
      grid --> csv
      csv.output

      val lines = Source.fromFile(file).getLines
      lines.toSet === Set("date|time",
        "2015-01-15|2015-04-03 12:30:00.0",
        "2014-10-05|2015-02-12 09:15:00.0",
        "2015-03-21|2015-01-30 05:10:00.0")
    }
    "work for mixed data" in {
      val file = createTestFile
      val csv = TextFileOutput(file.getPath, "name" -> "\"%-5s\"", "ssn" -> "%s", "weight" -> "%6.2f")
      grid --> csv
      csv.output

      val lines = Source.fromFile(file).getLines
      lines.toSet === Set("name,ssn,weight",
        "\"John \",111-22-3333,165.30",
        "\"Jane \",222-33-4444,124.00",
        "\"Jake \",333-00-1111,192.40")
    }
    "save to/load from xml" in {
      val csv = TextFileOutput("/tmp/myfile") % ("ssn" -> "%12s", "name" -> "%-5s") separator "|" header false
      csv.toXml must ==/(
        <text-file-output outputHeader="false">
          <path>/tmp/myfile</path>
          <separator>|</separator>
          <fields>
            <field name="ssn">%12s</field>
            <field name="name">%-5s</field>
          </fields>
        </text-file-output>)
      TextFileOutput.fromXml(csv.toXml) === csv
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._

      val csv = TextFileOutput("/tmp/myfile") % ("ssn" -> "%12s", "name" -> "%-5s") separator "|" header false
      csv.toJson === ("tag" -> "text-file-output") ~ ("outputHeader" -> false) ~
        ("path" -> "/tmp/myfile") ~ ("separator" -> "|") ~ ("fields" -> List(
          ("name" -> "ssn") ~ ("format" -> "%12s"),
          ("name" -> "name") ~ ("format" -> "%-5s")))
      TextFileOutput.fromJson(csv.toJson) === csv
    }
  }

  private def createTestFile = {
    val file = File.createTempFile("test", ".csv")
    file.deleteOnExit
    file
  }
}