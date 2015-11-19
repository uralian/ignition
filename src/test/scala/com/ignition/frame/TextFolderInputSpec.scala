package com.ignition.frame

import java.io.{ File, PrintWriter }

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.BeforeAllAfterAll
import com.ignition.types.{ fieldToRichStruct, string }

@RunWith(classOf[JUnitRunner])
class TextFolderInputSpec extends FrameFlowSpecification with BeforeAllAfterAll {

  private val testDir = {
    val tmp = File.createTempFile("test", "test")
    val tempDir = tmp.getParentFile
    tmp.delete

    new File(tempDir, java.util.UUID.randomUUID.toString)
  }

  override def beforeAll = {
    super.beforeAll
    testDir.mkdir
  }

  override def afterAll = {
    testDir.delete
    super.afterAll
  }

  "TextFolderInput" should {
    "load text files" in {
      val files = createTestFiles(testDir)(5)
      val step = TextFolderInput(testDir.getPath)
      val output = step.output.collect.map(_.toSeq).map(_(1)).toSet
      output === Set(files(0).getName, files(1).getName, files(2).getName,
        files(3).getName, files(4).getName)
      assertSchema(string("filename") ~ string("content"), step, 0)
    }
    "save to/load from xml" in {
      val step = TextFolderInput("/mypath", "file", "data")
      step.toXml must ==/(<text-folder-input path="/mypath" nameField="file" dataField="data"/>)
      TextFolderInput.fromXml(step.toXml) === step
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._

      val step = TextFolderInput("/mypath", "file", "data")
      step.toJson === ("tag" -> "text-folder-input") ~ ("path" -> "/mypath") ~ ("nameField" -> "file") ~
        ("dataField" -> "data")
      TextFolderInput.fromJson(step.toJson) === step
    }
  }

  private def createTestFiles(dir: File)(count: Int) = 1 to count map (_ => createTestFile(dir))

  private def createTestFile(dir: File) = {
    val file = new File(dir, java.util.UUID.randomUUID.toString)
    val pw = new PrintWriter(file)
    pw.print(file.getName)
    pw.close
    file
  }
}