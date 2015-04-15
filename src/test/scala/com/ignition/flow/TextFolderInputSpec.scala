package com.ignition.flow

import java.io.{ File, PrintWriter }

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.eaio.uuid.UUID
import com.ignition.BeforeAllAfterAll
import com.ignition.types.{ fieldToStruct, string }

@RunWith(classOf[JUnitRunner])
class TextFolderInputSpec extends FlowSpecification with BeforeAllAfterAll {

  private val testDir = {
    val tmp = File.createTempFile("test", "test")
    val tempDir = tmp.getParentFile
    tmp.delete

    new File(tempDir, new UUID().toString)
  }

  override def beforeAll = {
    super.beforeAll
    testDir.mkdir
  }

  override def afterAll = {
    testDir.delete
    super.afterAll
  }

  "TextFileInput" should {
    "load text files" in {
      val files = createTestFiles(testDir)(5)
      val step = TextFolderInput(testDir.getPath)
      val output = step.output.collect.map(_.toSeq).map(_(1)).toSet
      output === Set(files(0).getName, files(1).getName, files(2).getName,
        files(3).getName, files(4).getName)
      assertSchema(string("filename") ~ string("content"), step, 0)
    }
  }

  private def createTestFiles(dir: File)(count: Int) = 1 to count map (_ => createTestFile(dir))

  private def createTestFile(dir: File) = {
    val file = new File(dir, new UUID().toString)
    val pw = new PrintWriter(file)
    pw.print(file.getName)
    pw.close
    file
  }
}