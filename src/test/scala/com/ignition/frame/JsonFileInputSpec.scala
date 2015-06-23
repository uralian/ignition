package com.ignition.frame

import java.io.{ File, PrintWriter }

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.ExecutionException
import com.ignition.types.{ RichStructType, boolean, double, fieldToRichStruct, long, string }

@RunWith(classOf[JUnitRunner])
class JsonFileInputSpec extends FrameFlowSpecification {

  "JsonFileInput" should {
    "work with valid json files" in {
      val file = createTestFile(5)

      val columns = List("date" -> "date.value", "flag" -> "flag", "index" -> "index", "score" -> "score")
      val jfi = JsonFileInput(file.getPath, columns)

      assertSchema(string("date") ~ boolean("flag") ~ long("index") ~ double("score"), jfi, 0)
      assertOutput(jfi, 0, ("2015-04-01", false, 1, 0.2),
        ("2015-04-02", false, 2, 0.4), ("2015-04-03", true, 3, 0.6),
        ("2015-04-04", false, 4, 0.8), ("2015-04-05", false, 5, 1.0))
    }
    "fail for invalid json" in {
      val file = File.createTempFile("test", ".json")
      file.deleteOnExit
      val pw = new PrintWriter(file)
      pw.println("invalid content")
      pw.close

      val columns = List("date" -> "date.value", "flag" -> "flag", "index" -> "index", "score" -> "score")
      val jfi = JsonFileInput(file.getPath, columns)

      jfi.output must throwA[ExecutionException]
    }
    "be unserializable" in assertUnserializable(JsonFileInput("test"))
  }

  private def createTestFile(lines: Int) = {
    val file = File.createTempFile("test", ".json")
    file.deleteOnExit

    val pw = new PrintWriter(file)

    (1 to lines) foreach { index =>
      val name = s"John-$index"
      val date = f"2015-04-${index}%02d"
      val flag = index % 3 == 0
      val score = index.toDouble / lines
      pw.println(s"""{"index":$index, "name":"$name", "date":{"value":"$date"}, "flag":$flag, "score":$score}""")
    }

    pw.close
    file
  }
}