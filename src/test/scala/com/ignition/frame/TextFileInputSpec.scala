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
      val tfi = TextFileInput(file.getPath, Some("\\s".r))
      assertSchema(string("content"), tfi, 0)
      tfi.output.count === 5
      tfi.output.collect forall (_.getAs[String](0).size === 20)
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