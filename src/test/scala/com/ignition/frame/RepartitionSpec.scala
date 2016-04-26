package com.ignition.frame

import org.apache.spark.sql.Row
import org.json4s.JsonDSL
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RepartitionSpec extends FrameFlowSpecification {
  "Repartition" should {
    "increase the number of partitions" in {
      val range = RangeInput()
      val count = range.output.rdd.partitions.size
      val rep = Repartition(count * 2) shuffle true

      range --> rep

      val rows = Seq.range(0, 100, 1) map (Row(_))
      assertOutput(rep, 0, rows: _*)
      rep.output.rdd.partitions.size === count * 2
    }
    "decrease the number of partitions" in {
      val range = RangeInput()
      val count = range.output.rdd.partitions.size
      val rep = Repartition(count / 2) shuffle false

      range --> rep

      val rows = Seq.range(0, 100, 1) map (Row(_))
      assertOutput(rep, 0, rows: _*)
      rep.output.rdd.partitions.size === count / 2
    }
    "save to/load from xml" in {
      val step = Repartition(50) shuffle true
      step.toXml must ==/(<repartition size="50" shuffle="true"/>)
      Repartition.fromXml(step.toXml) === step
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._

      val step = Repartition(10) shuffle false
      step.toJson === ("tag" -> "repartition") ~ ("size" -> 10) ~ ("shuffle" -> false)
      Repartition.fromJson(step.toJson) === step
    }
    "be unserializable" in assertUnserializable(Repartition(20))
  }
}