package com.ignition.stream

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.types.{ RichStructType, double, fieldToRichStruct, int, string }
import com.ignition.util.JsonUtils.RichJValue
import com.ignition._
import com.ignition.frame.StringToLiteral

@RunWith(classOf[JUnitRunner])
class StreamFlowSpec extends StreamFlowSpecification {
  sequential

  import StreamFlow._

  val schema = string("name") ~ int("item") ~ double("score")
  val queue = QueueInput(schema).
    addRows(("john", 1, 65.0), ("john", 3, 78.0), ("jane", 2, 85.0)).
    addRows(("jane", 1, 46.0), ("jake", 4, 62.0), ("john", 3, 94.0))
  val sql = stream.foreach { frame.SQLQuery("SELECT name, AVG(score) as score FROM input0 GROUP BY name") }
  val select = stream.foreach { frame.SelectValues() rename ("score" -> "avg_score") }
  val stats = stream.foreach { frame.BasicStats() groupBy "name" add ("item", frame.BasicAggregator.COUNT) }
  val filter = Filter($"avg_score" > 80)

  val flow = StreamFlow {
    queue --> sql
    sql --> select --> filter
    queue --> stats
    (filter, stats)
  }

  "StreamFlow" should {
    "enumerate its steps and connections" in {
      flow.steps === Set(queue, sql, select, stats, filter)
      flow.connections === Set(
        Connection(queue, 0, sql, 0),
        Connection(sql, 0, select, 0),
        Connection(select, 0, filter, 0),
        Connection(queue, 0, stats, 0))
    }
    "yield the target outputs" in {
      runAndAssertOutput(flow, 0, 2, Set(("jane", 85)), Set(("john", 94)))
      runAndAssertOutput(flow, 1, 2, Set(("john", 71.5)), Set(("jane", 46.0), ("jake", 62.0)))
      runAndAssertOutput(flow, 2, 2, Set(("john", 2), ("jane", 1)), Set(("jane", 1), ("jake", 1), ("john", 1)))
    }
    "save to/load from xml" in {
      val xml = flow.toXml
      (xml \ "steps" \ "_").length === 5
      (xml \ "steps" \ Filter.tag).length === 1
      (xml \ "steps" \ QueueInput.tag).length === 1
      (xml \ "steps" \ Foreach.tag).length === 3
      (xml \ "steps" \ Foreach.tag \\ frame.SQLQuery.tag).length === 1
      (xml \ "steps" \ Foreach.tag \\ frame.SelectValues.tag).length === 1
      (xml \ "steps" \ Foreach.tag \\ frame.BasicStats.tag).length === 1
      val flow2 = StreamFlow.fromXml(xml)
      runAndAssertOutput(flow2, 0, 2, Set(("jane", 85)), Set(("john", 94)))
      runAndAssertOutput(flow2, 1, 2, Set(("john", 71.5)), Set(("jane", 46.0), ("jake", 62.0)))
      runAndAssertOutput(flow2, 2, 2, Set(("john", 2), ("jane", 1)), Set(("jane", 1), ("jake", 1), ("john", 1)))
    }
    "save to/load from json" in {
      import org.json4s._
      import org.json4s.JsonDSL._

      val json = flow.toJson
      (json \ "steps" asArray).length === 5
      (json \ "steps").find(x => (x \ "tag" asString) == Filter.tag) must beSome
      (json \ "steps").find(x => (x \ "tag" asString) == QueueInput.tag) must beSome
      (json \ "steps").find(x => (x \ "tag" asString) == Foreach.tag) must beSome
      val flow2 = StreamFlow.fromJson(json)
      runAndAssertOutput(flow2, 0, 2, Set(("jane", 85)), Set(("john", 94)))
      runAndAssertOutput(flow2, 1, 2, Set(("john", 71.5)), Set(("jane", 46.0), ("jake", 62.0)))
      runAndAssertOutput(flow2, 2, 2, Set(("john", 2), ("jane", 1)), Set(("jane", 1), ("jake", 1), ("john", 1)))
    }    
  }
}