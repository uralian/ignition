package com.ignition.stream

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.frame.StringToLiteral
import com.ignition.types._

@RunWith(classOf[JUnitRunner])
class FilterSpec extends StreamFlowSpecification {
  sequential

  val schema = string("name") ~ int("item") ~ double("score")

  val queue = QueueInput(schema).
    addRows(("john", 1, 65.0), ("john", 3, 78.0)).
    addRows(("jane", 2, 85.0), ("jane", 1, 46.0)).
    addRows(("jake", 4, 62.0), ("john", 3, 95.0))

  "Filter for numeric expressions" should {
    "evaluate `===`" in {
      val f = Filter($"item" === 1)
      queue --> f

      runAndAssertOutput(f, 0, 3, Set(("john", 1, 65.0)), Set(("jane", 1, 46.0)), Set())
      runAndAssertOutput(f, 1, 3, Set(("john", 3, 78.0)), Set(("jane", 2, 85.0)),
        Set(("jake", 4, 62.0), ("john", 3, 95.0)))
    }
    "evaluate `<`" in {
      val f = Filter($"score" < 50)
      queue --> f

      runAndAssertOutput(f, 0, 3, Set(), Set(("jane", 1, 46.0)), Set())
      runAndAssertOutput(f, 1, 3, Set(("john", 1, 65.0), ("john", 3, 78.0)),
        Set(("jane", 2, 85.0)), Set(("jake", 4, 62.0), ("john", 3, 95.0)))
    }
    "evaluate `>`" in {
      val f = Filter($"item" > 3)
      queue --> f

      runAndAssertOutput(f, 0, 3, Set(), Set(), Set(("jake", 4, 62.0)))
      runAndAssertOutput(f, 1, 3, Set(("john", 1, 65.0), ("john", 3, 78.0)),
        Set(("jane", 2, 85.0), ("jane", 1, 46.0)), Set(("john", 3, 95.0)))
    }
    "evaluate `<=`" in {
      val f = Filter($"item" <= 3)
      queue --> f

      runAndAssertOutput(f, 0, 3, Set(("john", 1, 65.0), ("john", 3, 78.0)),
        Set(("jane", 2, 85.0), ("jane", 1, 46.0)), Set(("john", 3, 95.0)))
      runAndAssertOutput(f, 1, 3, Set(), Set(), Set(("jake", 4, 62.0)))
    }
    "evaluate `>=`" in {
      val f = Filter($"score" >= 50)
      queue --> f

      runAndAssertOutput(f, 0, 3, Set(("john", 1, 65.0), ("john", 3, 78.0)),
        Set(("jane", 2, 85.0)), Set(("jake", 4, 62.0), ("john", 3, 95.0)))
      runAndAssertOutput(f, 1, 3, Set(), Set(("jane", 1, 46.0)), Set())
    }
    "evaluate `!==`" in {
      val f = Filter($"item" !== 1)
      queue --> f

      runAndAssertOutput(f, 0, 3, Set(("john", 3, 78.0)), Set(("jane", 2, 85.0)),
        Set(("jake", 4, 62.0), ("john", 3, 95.0)))
      runAndAssertOutput(f, 1, 3, Set(("john", 1, 65.0)), Set(("jane", 1, 46.0)), Set())
    }
  }

  "Filter for string expressions" should {
    "evaluate `===`" in {
      val f = Filter($"name" === "'john'")
      queue --> f

      runAndAssertOutput(f, 0, 3, Set(("john", 1, 65.0), ("john", 3, 78.0)),
        Set(), Set(("john", 3, 95.0)))
      runAndAssertOutput(f, 1, 3, Set(), Set(("jane", 2, 85.0), ("jane", 1, 46.0)),
        Set(("jake", 4, 62.0)))
    }
    "evaluate `!==`" in {
      val f = Filter($"name" !== "'john'")
      queue --> f

      runAndAssertOutput(f, 0, 3, Set(), Set(("jane", 2, 85.0), ("jane", 1, 46.0)),
        Set(("jake", 4, 62.0)))
      runAndAssertOutput(f, 1, 3, Set(("john", 1, 65.0), ("john", 3, 78.0)),
        Set(), Set(("john", 3, 95.0)))
    }
    "evaluate `matches`" in {
      val f = Filter($"name" rlike "'ja.e'")
      queue --> f

      runAndAssertOutput(f, 0, 3, Set(), Set(("jane", 2, 85.0), ("jane", 1, 46.0)),
        Set(("jake", 4, 62.0)))
      runAndAssertOutput(f, 1, 3, Set(("john", 1, 65.0), ("john", 3, 78.0)),
        Set(), Set(("john", 3, 95.0)))
    }
  }

  "Filter for complex expressions" should {
    "evaluate `and`" in {
      val f = Filter("item = 1 and score > 50")
      queue --> f

      runAndAssertOutput(f, 0, 3, Set(("john", 1, 65.0)), Set(), Set())
      runAndAssertOutput(f, 1, 3, Set(("john", 3, 78.0)),
        Set(("jane", 2, 85.0), ("jane", 1, 46.0)),
        Set(("jake", 4, 62.0), ("john", 3, 95.0)))
    }
    "evaluate `or`" in {
      val f = Filter("score < 70 or name rlike 'jo.*'")
      queue --> f

      runAndAssertOutput(f, 0, 3, Set(("john", 1, 65.0), ("john", 3, 78.0)),
        Set(("jane", 1, 46.0)), Set(("jake", 4, 62.0), ("john", 3, 95.0)))
      runAndAssertOutput(f, 1, 3, Set(), Set(("jane", 2, 85.0)), Set())
    }
    "evaluate `!`" in {
      val f = Filter(!($"item" > 3))
      queue --> f

      runAndAssertOutput(f, 0, 3, Set(("john", 1, 65.0), ("john", 3, 78.0)),
        Set(("jane", 2, 85.0), ("jane", 1, 46.0)), Set(("john", 3, 95.0)))
      runAndAssertOutput(f, 1, 3, Set(), Set(), Set(("jake", 4, 62.0)))
    }
  }

  val schema3 = string("login") ~ string("name") ~ int("item") ~ double("score")
  val queue3 = QueueInput(schema3).
    addRows(("john", "john q", 25, 15.5), ("jake", "jake", 13, 13.0)).
    addRows(("jane", "Jane", 9, 0.5), ("jack", "j j", 7, 12.3))

  "Filter for field-field expressions" should {
    "evaluate numeric fields" in {
      val f = Filter($"item" > $"score")
      queue3 --> f

      runAndAssertOutput(f, 0, 2, Set(("john", "john q", 25, 15.5)), Set(("jane", "Jane", 9, 0.5)))
      runAndAssertOutput(f, 1, 2, Set(("jake", "jake", 13, 13.0)), Set(("jack", "j j", 7, 12.3)))
    }
    "evaluate string fields" in {
      val f = Filter($"name" === $"login")
      queue3 --> f

      runAndAssertOutput(f, 0, 2, Set(("jake", "jake", 13, 13.0)), Set())
      runAndAssertOutput(f, 1, 2, Set(("john", "john q", 25, 15.5)),
        Set(("jane", "Jane", 9, 0.5), ("jack", "j j", 7, 12.3)))
    }
  }

  "Filter" should {
    "save to/load from xml" in {
      import com.ignition.util.XmlUtils._

      val f1 = Filter(($"score" < 70) or ($"name" rlike "'jo.*'"))
      (f1.toXml \ "condition" asString) === "((score < 70) || (name RLIKE 'jo.*'))"
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._

      val f1 = Filter(($"score" < 70) or ($"name" rlike "'jo.*'"))
      f1.toJson === ("tag" -> "stream-filter") ~ ("condition" -> "((score < 70) || (name RLIKE 'jo.*'))")
    }
  }
}