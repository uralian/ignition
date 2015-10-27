package com.ignition.stream

import org.json4s.JsonDSL
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.types.{ RichStructType, fieldToRichStruct, int, string }

@RunWith(classOf[JUnitRunner])
class StateUpdateSpec extends StreamFlowSpecification {
  sequential

  val schema = string("name") ~ string("topic") ~ int("score")

  val queue = QueueInput(schema).
    addRows(("john", "a", 65), ("jake", "c", 78)).
    addRows(("jane", "b", 55), ("jane", "a", 76), ("jake", "b", 84)).
    addRows().
    addRows(("jill", "b", 77), ("john", "c", 95)).
    addRows(("jake", "c", 86), ("jake", "b", 59), ("jane", "d", 88)).
    addRows(("john", "d", 97)).
    addRows(("josh", "a", 84), ("jane", "c", 65))

  "MvelMapState" should {
    val stateSchema = string("topic") ~ int("score")
    val expr = """$input.empty ? $state : $input[$input.size() - 1]"""
    "produce Map state stream" in {
      val step = MvelMapStateUpdate(stateSchema, expr, "name")
      queue.copy() --> step
      runAndAssertOutput(step, 0, 4,
        Set(("john", "a", 65), ("jake", "c", 78)),
        Set(("jane", "a", 76), ("jake", "b", 84), ("john", "a", 65)),
        Set(("jane", "a", 76), ("jake", "b", 84), ("john", "a", 65)),
        Set(("jane", "a", 76), ("jake", "b", 84), ("john", "c", 95), ("jill", "b", 77)))
    }
    "save to/load from xml" in {
      val step = MvelMapStateUpdate(stateSchema, expr, "name")
      step.toXml must ==/(
        <stream-state-mvel>
          <stateClass>Map</stateClass>
          <schema>
            <field name="topic" type="string" nullable="true"/>
            <field name="score" type="integer" nullable="true"/>
          </schema>
          <function><![CDATA[$input.empty ? $state : $input[$input.size() - 1]]]></function>
          <keys>
            <field name="name"/>
          </keys>
        </stream-state-mvel>)
      MvelStateUpdate.fromXml(step.toXml) === step
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._

      val step = MvelMapStateUpdate(stateSchema, expr, "name")
      step.toJson === ("tag" -> "stream-state-mvel") ~ ("stateClass" -> "Map") ~
        ("schema" -> List(
          ("name" -> "topic") ~ ("type" -> "string") ~ ("nullable" -> true),
          ("name" -> "score") ~ ("type" -> "integer") ~ ("nullable" -> true))) ~
          ("function" -> """$input.empty ? $state : $input[$input.size() - 1]""") ~
          ("keys" -> List("name"))
    }
  }

  "MvelMapListState" should {
    val stateSchema = int("score").schema
    val expr = """$input.empty ? $state : $input[$input.size() - 1]"""
    "produce Iterable state stream" in {
      val expr2 = """
      | newState = $state;
      | 
      | if ($input.size() > 0) {
      |   newRows = new java.util.ArrayList();
      |   foreach (row: $input)
      |     newRows.add(["score" : row.score]);
      |   
      |   if ($state == empty)
      |     newState = newRows;
      |   else
      |     newState.addAll(newRows);
      | }
      | 
      | newState;
      """.stripMargin
      val step = MvelMapListStateUpdate(stateSchema) code expr2 keys "topic"
      queue.copy() --> step
      runAndAssertOutput(step, 0, 3,
        Set(("a", 65), ("c", 78)),
        Set(("a", 65), ("c", 78), ("b", 55), ("a", 76), ("b", 84)),
        Set(("a", 65), ("c", 78), ("b", 55), ("a", 76), ("b", 84)))
    }
    "save to/load from xml" in {
      val step = MvelMapListStateUpdate(stateSchema) code expr keys "topic"
      step.toXml must ==/(
        <stream-state-mvel>
          <stateClass>Iterable</stateClass>
          <schema>
            <field name="score" type="integer" nullable="true"/>
          </schema>
          <function><![CDATA[$input.empty ? $state : $input[$input.size() - 1]]]></function>
          <keys>
            <field name="topic"/>
          </keys>
        </stream-state-mvel>)
      MvelStateUpdate.fromXml(step.toXml) === step
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._

      val step = MvelMapListStateUpdate(stateSchema) code expr keys "topic"
      step.toJson === ("tag" -> "stream-state-mvel") ~ ("stateClass" -> "Iterable") ~
        ("schema" -> List(
          ("name" -> "score") ~ ("type" -> "integer") ~ ("nullable" -> true))) ~
          ("function" -> """$input.empty ? $state : $input[$input.size() - 1]""") ~
          ("keys" -> List("topic"))
    }
  }
}