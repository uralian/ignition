package com.ignition.frame

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.types.{ fieldToStructType, string }

@RunWith(classOf[JUnitRunner])
class PassSpec extends FrameFlowSpecification {

  "Pass" should {
    "implement data passthrough" in {
      val grid = DataGrid(string("name")) rows (Tuple1("john"), Tuple1("jim"), Tuple1("jake"))
      val pass = Pass()
      grid --> pass
      assertOutput(pass, 0, Tuple1("john"), Tuple1("jim"), Tuple1("jake"))
      assertSchema(string("name"), pass, 0)
    }
    "save to/load from xml" in {
      val pass = Pass()
      pass.toXml must ==/(<pass/>)
      Pass.fromXml(pass.toXml) === pass
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._
      import org.json4s.JValue

      val pass = Pass()
      pass.toJson === (("tag" -> "pass"): JValue)
      Pass.fromJson(pass.toJson) === pass
    }
  }
}