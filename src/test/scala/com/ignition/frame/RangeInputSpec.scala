package com.ignition.frame

import org.apache.spark.sql.Row
import org.json4s.JsonDSL
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.types.{ fieldToStructType, long }

@RunWith(classOf[JUnitRunner])
class RangeInputSpec extends FrameFlowSpecification {

  "RangeInput" should {
    "generate numbers with default parameters" in {
      val step = RangeInput()
      val rows = Seq.range(0, 100, 1) map (Row(_))
      assertSchema(long("id", false), step, 0)
      assertOutput(step, 0, rows: _*)
    }
    "generate numbers with specified parameters" in {
      val step = RangeInput() start 5 end 15 step 2
      val rows = Seq(5, 7, 9, 11, 13) map (Row(_))
      assertSchema(long("id", false), step, 0)
      assertOutput(step, 0, rows: _*)
    }
    "save to/load from xml" in {
      val step = RangeInput() start 5 end 15
      step.toXml must ==/(
        <range-input>
          <start>5</start>
          <end>15</end>
          <step>1</step>
        </range-input>)
      RangeInput.fromXml(step.toXml) === step
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._

      val step = RangeInput() start 5 end 15 step 3
      step.toJson === ("tag" -> "range-input") ~ ("start" -> 5) ~ ("end" -> 15) ~ ("step" -> 3)
      RangeInput.fromJson(step.toJson) === step
    }
    "be unserializable" in assertUnserializable(RangeInput())
  }
}