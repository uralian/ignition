package com.ignition.frame

import org.apache.spark.sql.RichColumn
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.types.{ RichStructType, date, double, fieldToRichStruct, int, string, timestamp }
import com.ignition.util.XmlUtils

@RunWith(classOf[JUnitRunner])
class FilterSpec extends FrameFlowSpecification {
  sequential

  val schema = string("name") ~ int("item") ~ double("score")
  val grid = DataGrid(schema) rows (
    ("john", 1, 65.0), ("john", 3, 78.0), ("jane", 2, 85.0),
    ("jane", 1, 46.0), ("jake", 4, 62.0), ("john", 3, 95.0))

  "Filter for numeric expressions" should {
    "evaluate `===`" in {
      val f = Filter($"item" === 1)
      grid --> f
      assertOutput(f, 0, ("john", 1, 65.0), ("jane", 1, 46.0))
      assertOutput(f, 1, ("john", 3, 78.0), ("jane", 2, 85.0), ("jake", 4, 62.0), ("john", 3, 95.0))
    }
    "evaluate `<`" in {
      val f = Filter($"score" < 50)
      grid --> f
      assertOutput(f, 0, ("jane", 1, 46.0))
      assertOutput(f, 1, ("john", 1, 65.0), ("john", 3, 78.0), ("jane", 2, 85.0), ("jake", 4, 62.0), ("john", 3, 95.0))
    }
    "evaluate `>`" in {
      val f = Filter($"item" > 3)
      grid --> f
      assertOutput(f, 0, ("jake", 4, 62.0))
      assertOutput(f, 1, ("john", 1, 65.0), ("john", 3, 78.0), ("jane", 2, 85.0), ("jane", 1, 46.0), ("john", 3, 95.0))
    }
    "evaluate `<=`" in {
      val f = Filter($"item" <= 3)
      grid --> f
      assertOutput(f, 0, ("john", 1, 65.0), ("john", 3, 78.0), ("jane", 2, 85.0), ("jane", 1, 46.0), ("john", 3, 95.0))
      assertOutput(f, 1, ("jake", 4, 62.0))
    }
    "evaluate `>=`" in {
      val f = Filter($"score" >= 50)
      grid --> f
      assertOutput(f, 0, ("john", 1, 65.0), ("john", 3, 78.0), ("jane", 2, 85.0), ("jake", 4, 62.0), ("john", 3, 95.0))
      assertOutput(f, 1, ("jane", 1, 46.0))
    }
    "evaluate `!==`" in {
      val f = Filter($"item" !== 1)
      grid --> f
      assertOutput(f, 0, ("john", 3, 78.0), ("jane", 2, 85.0), ("jake", 4, 62.0), ("john", 3, 95.0))
      assertOutput(f, 1, ("john", 1, 65.0), ("jane", 1, 46.0))
    }
    "evaluate `IN`" in {
      val f = Filter($"item" IN (2, 4))
      grid --> f
      assertOutput(f, 0, ("jane", 2, 85.0), ("jake", 4, 62.0))
      assertOutput(f, 1, ("john", 1, 65.0), ("john", 3, 78.0), ("jane", 1, 46.0),
        ("john", 3, 95.0))
    }
  }

  "Filter for string expressions" should {
    "evaluate `===`" in {
      val f = Filter($"name" === "'john'")
      grid --> f
      assertOutput(f, 0, ("john", 1, 65.0), ("john", 3, 78.0), ("john", 3, 95.0))
      assertOutput(f, 1, ("jane", 2, 85.0), ("jane", 1, 46.0), ("jake", 4, 62.0))
    }
    "evaluate `!==`" in {
      val f = Filter($"name" !== "'john'")
      grid --> f
      assertOutput(f, 0, ("jane", 2, 85.0), ("jane", 1, 46.0), ("jake", 4, 62.0))
      assertOutput(f, 1, ("john", 1, 65.0), ("john", 3, 78.0), ("john", 3, 95.0))
    }
    "evaluate `like`" in {
      val f = Filter($"name" like "'ja%'")
      grid --> f
      assertOutput(f, 0, ("jane", 2, 85.0), ("jane", 1, 46.0), ("jake", 4, 62.0))
      assertOutput(f, 1, ("john", 1, 65.0), ("john", 3, 78.0), ("john", 3, 95.0))
    }
    "evaluate `rlike`" in {
      val f = Filter($"name" rlike "'ja.e'")
      grid --> f
      assertOutput(f, 0, ("jane", 2, 85.0), ("jane", 1, 46.0), ("jake", 4, 62.0))
      assertOutput(f, 1, ("john", 1, 65.0), ("john", 3, 78.0), ("john", 3, 95.0))
    }
    "evaluate `IN`" in {
      val f = Filter($"name" IN ("'jack'", "'jane'", "'jake'"))
      grid --> f
      assertOutput(f, 0, ("jane", 2, 85.0), ("jane", 1, 46.0), ("jake", 4, 62.0))
      assertOutput(f, 1, ("john", 1, 65.0), ("john", 3, 78.0), ("john", 3, 95.0))
    }
  }

  val schema2 = date("date") ~ timestamp("time")
  val grid2 = DataGrid(schema2) rows (
    (javaDate(1950, 12, 5), javaTime(1950, 12, 5, 12, 30)),
    (javaDate(1951, 2, 12), javaTime(1951, 2, 12, 9, 15)),
    (javaDate(1944, 7, 2), javaTime(1944, 7, 2, 17, 10)),
    (javaDate(1974, 4, 21), javaTime(1974, 4, 21, 23, 25)))

  "Filter for date expressions" should {
    "evaluate `===`" in {
      val f = Filter($"date" === "cast('1951-2-12' as date)")
      grid2 --> f
      assertOutput(f, 0, (javaDate(1951, 2, 12), javaTime(1951, 2, 12, 9, 15)))
      assertOutput(f, 1, (javaDate(1950, 12, 5), javaTime(1950, 12, 5, 12, 30)),
        (javaDate(1944, 7, 2), javaTime(1944, 7, 2, 17, 10)),
        (javaDate(1974, 4, 21), javaTime(1974, 4, 21, 23, 25)))
    }
    "evaluate `!==`" in {
      val f = Filter($"date" !== "cast('1951-2-12' as date)")
      grid2 --> f
      assertOutput(f, 0, (javaDate(1950, 12, 5), javaTime(1950, 12, 5, 12, 30)),
        (javaDate(1944, 7, 2), javaTime(1944, 7, 2, 17, 10)),
        (javaDate(1974, 4, 21), javaTime(1974, 4, 21, 23, 25)))
      assertOutput(f, 1, (javaDate(1951, 2, 12), javaTime(1951, 2, 12, 9, 15)))
    }
    "evaluate `<`" in {
      val f = Filter($"date" < "cast('1960-01-01' as date)")
      grid2 --> f
      assertOutput(f, 0, (javaDate(1950, 12, 5), javaTime(1950, 12, 5, 12, 30)),
        (javaDate(1951, 2, 12), javaTime(1951, 2, 12, 9, 15)),
        (javaDate(1944, 7, 2), javaTime(1944, 7, 2, 17, 10)))
      assertOutput(f, 1, (javaDate(1974, 4, 21), javaTime(1974, 4, 21, 23, 25)))
    }
    "evaluate `>`" in {
      val f = Filter($"date" > "cast('1951-02-10' as date)")
      grid2 --> f
      assertOutput(f, 0, (javaDate(1951, 2, 12), javaTime(1951, 2, 12, 9, 15)),
        (javaDate(1974, 4, 21), javaTime(1974, 4, 21, 23, 25)))
      assertOutput(f, 1, (javaDate(1950, 12, 5), javaTime(1950, 12, 5, 12, 30)),
        (javaDate(1944, 7, 2), javaTime(1944, 7, 2, 17, 10)))
    }
  }

  "Filter for complex expressions" should {
    "evaluate `and`" in {
      val f = Filter("item = 1 and score > 50")
      grid --> f
      assertOutput(f, 0, ("john", 1, 65.0))
    }
    "evaluate `or`" in {
      val f = Filter("score < 70 or name rlike 'jo.*'")
      grid --> f
      assertOutput(f, 0, ("john", 1, 65.0), ("john", 3, 78.0),
        ("jane", 1, 46.0), ("jake", 4, 62.0), ("john", 3, 95.0))
    }
    "evaluate `!`" in {
      val f = Filter(!($"item" > 3))
      grid --> f
      assertOutput(f, 0, ("john", 1, 65.0), ("john", 3, 78.0), ("jane", 2, 85.0),
        ("jane", 1, 46.0), ("john", 3, 95.0))
      assertOutput(f, 1, ("jake", 4, 62.0))
    }
  }

  val schema3 = string("login") ~ string("name") ~ int("item") ~ double("score")
  val grid3 = DataGrid(schema3) rows (
    ("john", "john q", 25, 15.5), ("jake", "jake", 13, 13.0),
    ("jane", "Jane", 9, 0.5), ("jack", "j j", 7, 12.3))

  "Filter for field-field expressions" should {
    "evaluate numeric fields" in {
      val f = Filter($"item" > $"score")
      grid3 --> f
      assertOutput(f, 0, ("john", "john q", 25, 15.5), ("jane", "Jane", 9, 0.5))
    }
    "evaluate string fields" in {
      val f = Filter($"name" === $"login")
      grid3 --> f
      assertOutput(f, 0, ("jake", "jake", 13, 13.0))
    }
  }

  "Filter" should {
    "save to/load from xml" in {
      import com.ignition.util.XmlUtils._

      val f1 = Filter(($"score" < 70) or ($"name" rlike "jo.*") and ($"date" > "cast('2015-01-02' as date)"))
      (f1.toXml \ "condition" asString) === "(((score < 70) || name RLIKE jo.*) && (date > cast('2015-01-02' as date)))"
      Filter.fromXml(f1.toXml).condition.toString === f1.condition.toString

      val f2 = Filter($"name" IN ("jack", "jane", "jake"))
      (f2.toXml \ "condition" asString) === "name IN (jack,jane,jake)"
      Filter.fromXml(f2.toXml).condition.toString === f2.condition.toString
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._

      val f1 = Filter(($"score" < 70) or ($"name" rlike "jo.*") and ($"date" > "cast('2015-01-02' as date)"))
      f1.toJson === ("tag" -> "filter") ~
        ("condition" -> "(((score < 70) || name RLIKE jo.*) && (date > cast('2015-01-02' as date)))")
      Filter.fromJson(f1.toJson).condition.toString === f1.condition.toString

      val f2 = Filter($"name" IN ("jack", "jane", "jake"))
      f2.toJson === ("tag" -> "filter") ~ ("condition" -> "name IN (jack,jane,jake)")
      Filter.fromJson(f2.toJson).condition.toString === f2.condition.toString
    }
    "be unserializable" in assertUnserializable(Filter($"item" === 1))
  }
}