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
    "evaluate `==`" in {
      val f = Filter($"item" == 1)
      queue --> f

      runAndAssertOutput(f, 0, 3, Set(("john", 1, 65.0)), Set(("jane", 1, 46.0)), Set())
      runAndAssertOutput(f, 1, 3, Set(("john", 3, 78.0)), Set(("jane", 2, 85.0)),
        Set(("jake", 4, 62.0), ("john", 3, 95.0)))

      assertSchema(schema, f, 0)
      assertSchema(schema, f, 1)
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
    "evaluate `<>`" in {
      val f = Filter($"item" <> 1)
      queue --> f

      runAndAssertOutput(f, 0, 3, Set(("john", 3, 78.0)), Set(("jane", 2, 85.0)),
        Set(("jake", 4, 62.0), ("john", 3, 95.0)))
      runAndAssertOutput(f, 1, 3, Set(("john", 1, 65.0)), Set(("jane", 1, 46.0)), Set())
    }
    "evaluate `in`" in {
      val f = Filter($"item" in (2, 4))
      queue --> f

      runAndAssertOutput(f, 0, 3, Set(), Set(("jane", 2, 85.0)), Set(("jake", 4, 62.0)))
      runAndAssertOutput(f, 1, 3, Set(("john", 1, 65.0), ("john", 3, 78.0)),
        Set(("jane", 1, 46.0)), Set(("john", 3, 95.0)))
    }
  }

  "Filter for string expressions" should {
    "evaluate `==`" in {
      val f = Filter($"name" == "john")
      queue --> f

      runAndAssertOutput(f, 0, 3, Set(("john", 1, 65.0), ("john", 3, 78.0)),
        Set(), Set(("john", 3, 95.0)))
      runAndAssertOutput(f, 1, 3, Set(), Set(("jane", 2, 85.0), ("jane", 1, 46.0)),
        Set(("jake", 4, 62.0)))
    }
    "evaluate `<>`" in {
      val f = Filter($"name" <> "john")
      queue --> f

      runAndAssertOutput(f, 0, 3, Set(), Set(("jane", 2, 85.0), ("jane", 1, 46.0)),
        Set(("jake", 4, 62.0)))
      runAndAssertOutput(f, 1, 3, Set(("john", 1, 65.0), ("john", 3, 78.0)),
        Set(), Set(("john", 3, 95.0)))
    }
    "evaluate `matches`" in {
      val f = Filter($"name" matches "ja.e")
      queue --> f

      runAndAssertOutput(f, 0, 3, Set(), Set(("jane", 2, 85.0), ("jane", 1, 46.0)),
        Set(("jake", 4, 62.0)))
      runAndAssertOutput(f, 1, 3, Set(("john", 1, 65.0), ("john", 3, 78.0)),
        Set(), Set(("john", 3, 95.0)))
    }
    "evaluate `in`" in {
      val f = Filter($"name" in ("jack", "jane", "jake"))
      queue --> f

      runAndAssertOutput(f, 0, 3, Set(), Set(("jane", 2, 85.0), ("jane", 1, 46.0)),
        Set(("jake", 4, 62.0)))
      runAndAssertOutput(f, 1, 3, Set(("john", 1, 65.0), ("john", 3, 78.0)), Set(),
        Set(("john", 3, 95.0)))
    }
  }

  val schema2 = date("date") ~ timestamp("time")
  val queue2 = QueueInput(schema2).
    addRows(
      (javaDate(1950, 12, 5), javaTime(1950, 12, 5, 12, 30)),
      (javaDate(1951, 2, 12), javaTime(1951, 2, 12, 9, 15))).
      addRows(
        (javaDate(1944, 7, 2), javaTime(1944, 7, 2, 17, 10)),
        (javaDate(1974, 4, 21), javaTime(1974, 4, 21, 23, 25)))

  "Filter for date expressions" should {
    "evaluate `==`" in {
      val f = Filter($"date" == javaDate(1951, 2, 12))
      queue2 --> f

      runAndAssertOutput(f, 0, 2, Set((javaDate(1951, 2, 12), javaTime(1951, 2, 12, 9, 15))), Set())
      runAndAssertOutput(f, 1, 2, Set((javaDate(1950, 12, 5), javaTime(1950, 12, 5, 12, 30))),
        Set((javaDate(1944, 7, 2), javaTime(1944, 7, 2, 17, 10)),
          (javaDate(1974, 4, 21), javaTime(1974, 4, 21, 23, 25))))
    }
    "evaluate `<>`" in {
      val f = Filter($"date" <> javaDate(1951, 2, 12))
      queue2 --> f

      runAndAssertOutput(f, 0, 2, Set((javaDate(1950, 12, 5), javaTime(1950, 12, 5, 12, 30))),
        Set((javaDate(1944, 7, 2), javaTime(1944, 7, 2, 17, 10)),
          (javaDate(1974, 4, 21), javaTime(1974, 4, 21, 23, 25))))
      runAndAssertOutput(f, 1, 2, Set((javaDate(1951, 2, 12), javaTime(1951, 2, 12, 9, 15))), Set())
    }
    "evaluate `<`" in {
      val f = Filter($"date" < javaDate(1960, 1, 1))
      queue2 --> f

      runAndAssertOutput(f, 0, 2, Set((javaDate(1950, 12, 5), javaTime(1950, 12, 5, 12, 30)),
        (javaDate(1951, 2, 12), javaTime(1951, 2, 12, 9, 15))),
        Set((javaDate(1944, 7, 2), javaTime(1944, 7, 2, 17, 10))))
      runAndAssertOutput(f, 1, 2, Set(),
        Set((javaDate(1974, 4, 21), javaTime(1974, 4, 21, 23, 25))))
    }
    "evaluate `>`" in {
      val f = Filter($"date" > javaDate(1951, 2, 10))
      queue2 --> f

      runAndAssertOutput(f, 0, 2, Set((javaDate(1951, 2, 12), javaTime(1951, 2, 12, 9, 15))),
        Set((javaDate(1974, 4, 21), javaTime(1974, 4, 21, 23, 25))))
      runAndAssertOutput(f, 1, 2, Set((javaDate(1950, 12, 5), javaTime(1950, 12, 5, 12, 30))),
        Set((javaDate(1944, 7, 2), javaTime(1944, 7, 2, 17, 10))))
    }
  }

  "Filter for complex expressions" should {
    "evaluate `and`" in {
      val f = Filter($"item" == 1 and $"score" > 50)
      queue --> f

      runAndAssertOutput(f, 0, 3, Set(("john", 1, 65.0)), Set(), Set())
      runAndAssertOutput(f, 1, 3, Set(("john", 3, 78.0)),
        Set(("jane", 2, 85.0), ("jane", 1, 46.0)),
        Set(("jake", 4, 62.0), ("john", 3, 95.0)))
    }
    "evaluate `or`" in {
      val f = Filter($"score" < 70 or $"name" ~ "jo.*")
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
      val f = Filter($"name" == $"login")
      queue3 --> f

      runAndAssertOutput(f, 0, 2, Set(("jake", "jake", 13, 13.0)), Set())
      runAndAssertOutput(f, 1, 2, Set(("john", "john q", 25, 15.5)),
        Set(("jane", "Jane", 9, 0.5), ("jack", "j j", 7, 12.3)))
    }
  }

  "Filter for field-var expressions" should {
    "evaluate numeric fields" in {
      val sv = SetVariables("threshold" -> 80)
      val f = Filter($"score" > v"threshold")
      queue --> sv --> f

      runAndAssertOutput(f, 0, 3, Set(), Set(("jane", 2, 85.0)), Set(("john", 3, 95.0)))
      runAndAssertOutput(f, 1, 3, Set(("john", 1, 65.0), ("john", 3, 78.0)),
        Set(("jane", 1, 46.0)), Set(("jake", 4, 62.0)))
    }
    "evaluate string fields" in {
      val sv = SetVariables("pattern" -> "ja.*")
      val f = Filter($"name" ~ v"pattern")
      queue --> sv --> f

      runAndAssertOutput(f, 0, 3, Set(), Set(("jane", 2, 85.0), ("jane", 1, 46.0)),
        Set(("jake", 4, 62.0)))
      runAndAssertOutput(f, 1, 3, Set(("john", 1, 65.0), ("john", 3, 78.0)), Set(),
        Set(("john", 3, 95.0)))
    }
  }

  "Filter for field-env expressions" should {
    "evaluate numeric fields" in {
      System.setProperty("threshold", "80")
      val f = Filter($"score" > e"threshold")
      queue --> f

      runAndAssertOutput(f, 0, 3, Set(), Set(("jane", 2, 85.0)), Set(("john", 3, 95.0)))
      runAndAssertOutput(f, 1, 3, Set(("john", 1, 65.0), ("john", 3, 78.0)),
        Set(("jane", 1, 46.0)), Set(("jake", 4, 62.0)))
    }
    "evaluate string fields" in {
      System.setProperty("pattern", "ja.*")
      val f = Filter($"name" ~ e"pattern")
      queue --> f

      runAndAssertOutput(f, 0, 3, Set(), Set(("jane", 2, 85.0), ("jane", 1, 46.0)),
        Set(("jake", 4, 62.0)))
      runAndAssertOutput(f, 1, 3, Set(("john", 1, 65.0), ("john", 3, 78.0)), Set(),
        Set(("john", 3, 95.0)))
    }
  }
}