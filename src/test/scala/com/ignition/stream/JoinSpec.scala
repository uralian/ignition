package com.ignition.stream

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.frame.StringToLiteral
import com.ignition.frame.JoinType._

import com.ignition.types._

@RunWith(classOf[JUnitRunner])
class JoinSpec extends StreamFlowSpecification {
  sequential

  val schema1 = string("name") ~ int("item") ~ double("score")
  val queue1 = QueueInput(schema1).
    addRows(("john", 1, 15.0), ("john", 3, 10.0)).
    addRows(("jake", 4, 25.0), ("john", 3, 30.0))

  val schema2 = string("name") ~ int("age") ~ boolean("flag")
  val queue2 = QueueInput(schema2).
    addRows(("jake", 20, true), ("john", 15, false)).
    addRows(("jane", 10, true), ("john", 30, true))

  "Join without condition" should {
    "produce Cartesian project" in {
      val join = Join()
      (queue1, queue2) --> join

      runAndAssertOutput(join, 0, 2,
        Set(("john", 1, 15.0, "jake", 20, true), ("john", 3, 10.0, "jake", 20, true),
          ("john", 1, 15.0, "john", 15, false), ("john", 3, 10.0, "john", 15, false)),
        Set(("jake", 4, 25.0, "jane", 10, true), ("john", 3, 30.0, "jane", 10, true),
          ("jake", 4, 25.0, "john", 30, true), ("john", 3, 30.0, "john", 30, true)))
    }
  }
  
  "Join" should {
    "save to/load from xml" in {
      import com.ignition.util.XmlUtils._

      val j1 = Join()
      j1.toXml must ==/(<stream-join type="inner"></stream-join>)
      Join.fromXml(j1.toXml) === j1

      val j2 = Join($"input0.name" === $"input1.name")
      j2.toXml must ==/(<stream-join type="inner"><condition>(input0.name = input1.name)</condition></stream-join>)
      Join.fromXml(j2.toXml).condition.toString === j2.condition.toString
      Join.fromXml(j2.toXml).joinType === j2.joinType

      val j3 = Join($"a" < $"b", OUTER)
      (j3.toXml \ "condition" asString) === "(a < b)"
      Join.fromXml(j3.toXml).condition.toString === j3.condition.toString
      Join.fromXml(j3.toXml).joinType === j3.joinType
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._

      val j1 = Join()
      j1.toJson === ("tag" -> "stream-join") ~ ("type" -> "inner") ~ ("condition" -> jNone)
      Join.fromJson(j1.toJson) === j1

      val j2 = Join($"input0.name" === $"input1.name")
      j2.toJson === ("tag" -> "stream-join") ~ ("type" -> "inner") ~ ("condition" -> "(input0.name = input1.name)")
      Join.fromJson(j2.toJson).condition.map(_.toString) === j2.condition.map(_.toString)

      val j3 = Join($"a" < $"b", OUTER)
      j3.toJson === ("tag" -> "stream-join") ~ ("type" -> "outer") ~ ("condition" -> "(a < b)")
      Join.fromJson(j3.toJson).condition.map(_.toString) === j3.condition.map(_.toString)
    }
  }  
}