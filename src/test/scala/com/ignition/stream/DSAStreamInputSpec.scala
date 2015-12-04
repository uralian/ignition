package com.ignition.stream

import org.json4s.JsonDSL
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DSAStreamInputSpec extends StreamFlowSpecification {

  "DSAStreamInput" should {
    "save to/load from xml" in {
      val step = DSAStreamInput("path1" -> "string", "path2" -> "integer")
      step.toXml must ==/(
        <stream-dsa-input>
          <paths>
            <path type="string">path1</path>
            <path type="integer">path2</path>
          </paths>
        </stream-dsa-input>)
      DSAStreamInput.fromXml(step.toXml) === step
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._

      val step = DSAStreamInput("path1" -> "string", "path2" -> "integer")
      step.toJson === ("tag" -> "stream-dsa-input") ~ ("paths" -> List(
        ("type" -> "string") ~ ("path" -> "path1"),
        ("type" -> "integer") ~ ("path" -> "path2")))
      DSAStreamInput.fromJson(step.toJson) === step
    }
  }
}