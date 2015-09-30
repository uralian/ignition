package com.ignition.frame

import java.io.{ File, PrintWriter }

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.types._

@RunWith(classOf[JUnitRunner])
class CsvFileInputSpec extends FrameFlowSpecification {

  val file = createTestFile(5)

  "CsvFileInput" should {
    "load CSV file without schema and separator" in {
      val csv = CsvFileInput(file.getPath) separator (None)
      assertSchema(string("COL0"), csv)
      assertOutput(csv, 0,
        Seq("1,John-1,2015-04-01,false"),
        Seq("2,John-2,2015-04-02,false"),
        Seq("3,John-3,2015-04-03,true"),
        Seq("4,John-4,2015-04-04,false"),
        Seq("5,John-5,2015-04-05,false"))
    }
    "load CSV file without schema with separator" in {
      val csv = CsvFileInput(file.getPath) separator (",")
      assertSchema(string("COL0") ~ string("COL1") ~ string("COL2") ~ string("COL3"), csv)
      assertOutput(csv, 0,
        Seq("1", "John-1", "2015-04-01", "false"),
        Seq("2", "John-2", "2015-04-02", "false"),
        Seq("3", "John-3", "2015-04-03", "true"),
        Seq("4", "John-4", "2015-04-04", "false"),
        Seq("5", "John-5", "2015-04-05", "false"))
    }
    "load CSV file without schema with separator" in {
      val csv = CsvFileInput(file.getPath) separator (None) schema (string("main"))
      assertSchema(string("main"), csv)
      assertOutput(csv, 0,
        Seq("1,John-1,2015-04-01,false"),
        Seq("2,John-2,2015-04-02,false"),
        Seq("3,John-3,2015-04-03,true"),
        Seq("4,John-4,2015-04-04,false"),
        Seq("5,John-5,2015-04-05,false"))
    }
    "load CSV file with schema with separator" in {
      val schema = int("index") ~ string("name") ~ date("when") ~ boolean("flag")
      val csv = CsvFileInput(file.getPath, ",", schema)
      assertOutput(csv, 0,
        Seq(1, "John-1", javaDate(2015, 4, 1), false),
        Seq(2, "John-2", javaDate(2015, 4, 2), false),
        Seq(3, "John-3", javaDate(2015, 4, 3), true),
        Seq(4, "John-4", javaDate(2015, 4, 4), false),
        Seq(5, "John-5", javaDate(2015, 4, 5), false))
      assertSchema(schema, csv, 0)
    }
    "save to xml/load from xml" in {
      val c1 = CsvFileInput("file", None, None)
      c1.toXml must ==/(<csv-file-input><path>file</path></csv-file-input>)
      CsvFileInput.fromXml(c1.toXml) === c1

      val c2 = CsvFileInput("file", Some(","), None)
      c2.toXml must ==/(<csv-file-input><path>file</path><separator>,</separator></csv-file-input>)
      CsvFileInput.fromXml(c2.toXml) === c2

      val c3 = CsvFileInput("file", None, Some(int("index") ~ date("when")))
      c3.toXml must ==/(
        <csv-file-input>
          <path>file</path>
          <schema>
            <field name="index" type="integer" nullable="true"/>
            <field name="when" type="date" nullable="true"/>
          </schema>
        </csv-file-input>)
      CsvFileInput.fromXml(c3.toXml) === c3
    }
    "save to xml/load from json" in {
      import org.json4s.JsonDSL._

      val c1 = CsvFileInput("file", None, None)
      c1.toJson === ("tag" -> "csv-file-input") ~ ("path" -> "file") ~
        ("separator" -> jNone) ~ ("schema" -> jNone)
      CsvFileInput.fromJson(c1.toJson) === c1

      val c2 = CsvFileInput("file", Some(","), None)
      c2.toJson === ("tag" -> "csv-file-input") ~ ("path" -> "file") ~
        ("separator" -> ",") ~ ("schema" -> jNone)
      CsvFileInput.fromJson(c2.toJson) === c2

      val c3 = CsvFileInput("file", None, Some(int("index") ~ date("when")))
      c3.toJson === ("tag" -> "csv-file-input") ~ ("path" -> "file") ~
        ("separator" -> jNone) ~ ("schema" -> List(
          ("name" -> "index") ~ ("type" -> "integer") ~ ("nullable" -> true),
          ("name" -> "when") ~ ("type" -> "date") ~ ("nullable" -> true)))
      CsvFileInput.fromJson(c3.toJson) === c3
    }
  }

  private def createTestFile(lines: Int) = {
    val file = File.createTempFile("test", ".csv")
    file.deleteOnExit
    val pw = new PrintWriter(file)
    (1 to lines) foreach { index =>
      val name = s"John-$index"
      val date = f"2015-04-${index}%02d"
      val flag = index % 3 == 0
      pw.println(s"$index,$name,$date,$flag")
    }
    pw.close
    file
  }
}