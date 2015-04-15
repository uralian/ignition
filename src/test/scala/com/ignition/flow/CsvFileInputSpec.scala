package com.ignition.flow

import java.io.{ File, PrintWriter }

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.types._

@RunWith(classOf[JUnitRunner])
class CsvFileInputSpec extends FlowSpecification {

  val file = createTestFile(5)

  "CsvFileInput" should {
    "load CSV file" in {
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