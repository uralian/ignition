package com.ignition.frame

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.types.{ RichStructType, date, fieldToRichStruct, int, string }

@RunWith(classOf[JUnitRunner])
class DebugOutputSpec extends FrameFlowSpecification {

  private val newid = "39abc670-5386-11e5-b7ab-d61480493bf3"

  "DebugOutput" should {
    "output frame data with names, types and title" in {
      val grid = DataGrid(string("id") ~ string("name") ~ int("weight") ~ date("dob")) rows (
        (newid, "john", 155, javaDate(1980, 5, 2)),
        (newid, "jane", 190, javaDate(1982, 4, 25)),
        (newid, "jake", 160, javaDate(1974, 11, 3)),
        (newid, "josh", 120, javaDate(1995, 1, 10)))
      val debug = DebugOutput() showNames true showTypes true title "Summary"

      grid --> debug

      val baos = new java.io.ByteArrayOutputStream
      Console.withOut(baos) { debug.output }

      val lines = baos.toString.split("\\r?\\n")

      lines.length === 10
      lines(0) === "Summary"
      lines(2) === "|                                  id|  name| weight|       dob|"
      lines(3) === "|                              string|string|integer|      date|"
      lines(5) === "|39abc670-5386-11e5-b7ab-d61480493bf3|  john|    155|1980-05-02|"
      lines(6) === "|39abc670-5386-11e5-b7ab-d61480493bf3|  jane|    190|1982-04-25|"
      lines(7) === "|39abc670-5386-11e5-b7ab-d61480493bf3|  jake|    160|1974-11-03|"
      lines(8) === "|39abc670-5386-11e5-b7ab-d61480493bf3|  josh|    120|1995-01-10|"
    }
    "output frame data long lines" in {
      val grid = DataGrid(string("id") ~ string("name") ~ int("weight") ~ date("dob")) rows (
        (newid, "johnjohnjohnjohnjohnjohn", 111222333, javaDate(1980, 5, 2)),
        (newid, "janejanejanejanejanejane", 444555666, javaDate(1982, 4, 25)),
        (newid, "jakejakejakejakejakejake", 777888999, javaDate(1974, 11, 3)),
        (newid, "joshjoshjoshjoshjoshjosh", 111000000, javaDate(1995, 1, 10)))
      val debug = DebugOutput() showNames false showTypes false noTitle () maxWidth (50)

      grid --> debug

      val baos = new java.io.ByteArrayOutputStream
      Console.withOut(baos) { debug.output }

      val lines = baos.toString.split("\\r?\\n")

      lines.length === 6
      lines(1) === "|39abc670-5386-11e5-|johnjohnjohn|1112|1980-|"
      lines(2) === "|39abc670-5386-11e5-|janejanejane|4445|1982-|"
      lines(3) === "|39abc670-5386-11e5-|jakejakejake|7778|1974-|"
      lines(4) === "|39abc670-5386-11e5-|joshjoshjosh|1110|1995-|"
    }
    "save to/load from xml" in {
      val d1 = DebugOutput() showNames false showTypes false noTitle () unlimitedWidth ()
      d1.toXml must ==/(<debug-output names="false" types="false"/>)
      DebugOutput.fromXml(d1.toXml) === d1

      val d2 = DebugOutput() showNames true showTypes true title "debug" maxWidth 100
      d2.toXml must ==/(
        <debug-output names="true" types="true" max-width="100">
          <title>debug</title>
        </debug-output>)
      DebugOutput.fromXml(d2.toXml) === d2
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._

      val d1 = DebugOutput() showNames false showTypes false noTitle () unlimitedWidth ()
      d1.toJson === ("tag" -> "debug-output") ~ ("names" -> false) ~ ("types" -> false) ~
        ("title" -> jNone) ~ ("maxWidth" -> jNone)
      DebugOutput.fromJson(d1.toJson) === d1

      val d2 = DebugOutput() showNames true showTypes true title "debug" maxWidth 100
      d2.toJson === ("tag" -> "debug-output") ~ ("names" -> true) ~ ("types" -> true) ~
        ("title" -> "debug") ~ ("maxWidth" -> 100)
      DebugOutput.fromJson(d2.toJson) === d2
    }
  }
}