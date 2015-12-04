package com.ignition.frame

import org.json4s.JsonDSL
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DSAInputSpec extends FrameFlowSpecification {

  "DSAInput" should {
    "save to/load from xml" in {
      val step = DSAInput("path1" -> "string", "path2" -> "integer")
      step.toXml must ==/(
        <dsa-input>
          <paths>
            <path type="string">path1</path>
            <path type="integer">path2</path>
          </paths>
        </dsa-input>)
      DSAInput.fromXml(step.toXml) === step
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._

      val step = DSAInput("path1" -> "string", "path2" -> "integer")
      step.toJson === ("tag" -> "dsa-input") ~ ("paths" -> List(
        ("type" -> "string") ~ ("path" -> "path1"),
        ("type" -> "integer") ~ ("path" -> "path2")))
      DSAInput.fromJson(step.toJson) === step
    }
    "be unserializable" in assertUnserializable(DSAInput("path1" -> "string", "path2" -> "integer"))
  }
}