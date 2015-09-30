package com.ignition.frame

import java.io.{ File, PrintWriter }
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import com.ignition.types._
import scala.util.matching.Regex
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TextFileInputSpec extends FrameFlowSpecification {

  "TextFileInput" should {
    "read file without separator" in {
      val file = createTestFile(20, 5, "")
      val tfi = TextFileInput(file.getPath, None, "data")
      assertSchema(string("data"), tfi, 0)
      tfi.output.count === 1
      tfi.output.collect.head.getAs[String](0).size === 20 * 5
    }
    "read file with separator" in {
      val file = createTestFile(20, 5, "\n")
      val tfi = TextFileInput(file.getPath, Some("\\s"))
      assertSchema(string("content"), tfi, 0)
      tfi.output.count === 5
      tfi.output.collect forall (_.getAs[String](0).size === 20)
    }
    "save to/load from xml" in {
      val t1 = TextFileInput("/tmp/myfile", None, "data")
      t1.toXml must ==/(
        <text-file-input>
          <path>/tmp/myfile</path>
          <dataField>data</dataField>
        </text-file-input>)
      TextFileInput.fromXml(t1.toXml) === t1

      val t2 = TextFileInput("/tmp/myfile", Some("\\s"))
      t2.toXml must ==/(
        <text-file-input>
          <path>/tmp/myfile</path>
          <separator>\s</separator>
          <dataField>content</dataField>
        </text-file-input>)
      TextFileInput.fromXml(t2.toXml) === t2
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._

      val t1 = TextFileInput("/tmp/myfile", None, "data")
      t1.toJson === ("tag" -> "text-file-input") ~ ("path" -> "/tmp/myfile") ~ ("dataField" -> "data") ~
        ("separator" -> jNone)
      TextFileInput.fromJson(t1.toJson) === t1

      val t2 = TextFileInput("/tmp/myfile", Some("\\s"))
      t2.toJson === ("tag" -> "text-file-input") ~ ("path" -> "/tmp/myfile") ~ ("dataField" -> "content") ~
        ("separator" -> "\\s")
      TextFileInput.fromJson(t2.toJson) === t2
    }
  }

  private def createTestFile(blockSize: Int, blocks: Int, separator: String) = {
    val file = File.createTempFile("test", ".txt")
    file.deleteOnExit

    val pw = new PrintWriter(file)

    val it = Random.alphanumeric.iterator
    (1 to blocks) foreach { i =>
      val block = it.take(blockSize).mkString
      pw.print(block)
      pw.print(separator)
    }

    pw.close
    file
  }
}