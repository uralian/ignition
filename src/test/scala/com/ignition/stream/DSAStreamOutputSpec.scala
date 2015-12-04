package com.ignition.stream

import org.json4s.JsonDSL
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DSAStreamOutputSpec extends StreamFlowSpecification {

  "DSAStreamOutput" should {
    "save to/load from xml" in {
      val step = DSAStreamOutput("name1" -> "path1", "name2" -> "path2")
      step.toXml must ==/(
        <stream-dsa-output>
          <fields>
            <field name="name1">path1</field>
            <field name="name2">path2</field>
          </fields>
        </stream-dsa-output>)
      DSAStreamOutput.fromXml(step.toXml) === step
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._

      val step = DSAStreamOutput("name1" -> "path1", "name2" -> "path2")
      step.toJson === ("tag" -> "stream-dsa-output") ~ ("fields" -> List(
        ("name" -> "name1") ~ ("path" -> "path1"),
        ("name" -> "name2") ~ ("path" -> "path2")))
      DSAStreamOutput.fromJson(step.toJson) === step
    }
  }
}