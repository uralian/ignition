package com.ignition.frame

import org.json4s.JsonDSL
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DSAOutputSpec extends FrameFlowSpecification {

  "DSAStreamOutput" should {
    "save to/load from xml" in {
      val step = DSAOutput("name1" -> "path1", "name2" -> "path2")
      step.toXml must ==/(
        <dsa-output>
          <fields>
            <field name="name1">path1</field>
            <field name="name2">path2</field>
          </fields>
        </dsa-output>)
      DSAOutput.fromXml(step.toXml) === step
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._

      val step = DSAOutput("name1" -> "path1", "name2" -> "path2")
      step.toJson === ("tag" -> "dsa-output") ~ ("fields" -> List(
        ("name" -> "name1") ~ ("path" -> "path1"),
        ("name" -> "name2") ~ ("path" -> "path2")))
      DSAOutput.fromJson(step.toJson) === step
    }
    "be unserializable" in assertUnserializable(DSAOutput("name1" -> "path1", "name2" -> "path2"))
  }
}