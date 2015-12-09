package com.ignition.frame

import java.io.{ File, PrintWriter }

import org.json4s.JsonDSL
import org.json4s.jackson.JsonMethods.pretty
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.types.{ fieldToRichStruct, int, string }
import com.ignition.util.XmlUtils

@RunWith(classOf[JUnitRunner])
class InvokeSpec extends FrameFlowSpecification {

  "Invoke" should {
    "handle a producer step" in {
      val grid = DataGrid(string("name") ~ int("age")) rows (("john", 25), ("jane", 33))
      val file = serialize(grid)

      val inv = Invoke(file.getPath, "json")
      assertSchema(string("name") ~ int("age"), inv, 0)
      assertOutput(inv, 0, Seq("john", 25), Seq("jane", 33))
    }
    "handle a transformer step" in {
      val select = SelectValues() rename ("name" -> "first_name")
      val file = serialize(select)

      val grid = DataGrid(string("name") ~ int("age")) rows (("john", 25), ("jane", 33))
      val inv = Invoke(file.getPath, "json")
      grid --> inv

      assertSchema(string("first_name") ~ int("age"), inv, 0)
      assertOutput(inv, 0, Seq("john", 25), Seq("jane", 33))
    }
    "handle a splitter step" in {
      val filter = Filter("age < 30")
      val file = serialize(filter)

      val grid = DataGrid(string("name") ~ int("age")) rows (("john", 25), ("jane", 33))
      val inv = Invoke(file.getPath, "json")
      grid --> inv

      assertSchema(string("name") ~ int("age"), inv, 0)
      assertOutput(inv, 0, Seq("john", 25))
      assertOutput(inv, 1, Seq("jane", 33))
    }
    "handle a merger step" in {
      val sql = SQLQuery("select * from input0 where name='john'")
      val file = serialize(sql)

      val grid = DataGrid(string("name") ~ int("age")) rows (("john", 25), ("jane", 33))
      val inv = Invoke(file.getPath, "json")
      grid --> inv

      assertSchema(string("name") ~ int("age"), inv, 0)
      assertOutput(inv, 0, Seq("john", 25))
    }
    "save to/load from xml" in {
      import com.ignition.util.XmlUtils._

      val inv = Invoke("my_path", "json")
      inv.toXml must ==/(<invoke><path type="json">my_path</path></invoke>)
      Invoke.fromXml(inv.toXml) === inv
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._

      val inv = Invoke("my_path", "xml")
      inv.toJson === ("tag" -> "invoke") ~ ("path" -> "my_path") ~ ("type" -> "xml")
      Invoke.fromJson(inv.toJson) === inv
    }
    "be unserializable" in assertUnserializable(Invoke("path", "xml"))
  }

  private def serialize(step: FrameStep) = {
    val file = createTestFile("json")
    val pw = new PrintWriter(file)
    pw.println(pretty(step.toJson))
    pw.close
    file
  }

  private def createTestFile(fileType: String) = {
    val file = File.createTempFile("test", "." + fileType)
    file.deleteOnExit
    file
  }
}