package com.ignition.frame

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.RichProduct
import com.ignition.types.{ RichStructType, boolean, double, fieldToRichStruct, int, string }
import com.ignition.util.XmlUtils

@RunWith(classOf[JUnitRunner])
class JoinSpec extends FrameFlowSpecification {
  import JoinType._

  val schema1 = string("name") ~ int("item") ~ double("score")
  val grid1 = DataGrid(schema1) rows (
    ("john", 1, 25.0), ("jane", 1, 46.0), ("jake", 4, 62.0))

  val schema2 = string("name") ~ int("age") ~ boolean("flag")
  val grid2 = DataGrid(schema2) rows (
    ("john", 25, true), ("jane", 35, false), ("jill", 18, true))

  "Join without condition" should {
    "produce Cartesian product" in {
      val join = Join()
      (grid1, grid2) --> join

      join.output.count === grid1.rows.size * grid2.rows.size
      assertSchema(schema1 ~~ schema2, join, 0)
    }
  }

  "INNER Join" should {
    "work with equality expressions" in {
      val join = Join($"input0.name" === $"input1.name")
      (grid1, grid2) --> join
      assertOutput(join, 0, ("john", 1, 25.0, "john", 25, true),
        ("jane", 1, 46.0, "jane", 35, false))
    }
    "work with inequality expressions" in {
      val join = Join($"score" < 40)
      (grid1, grid2) --> join

      assertOutput(join, 0, ("john", 1, 25.0, "john", 25, true),
        ("john", 1, 25.0, "jane", 35, false), ("john", 1, 25.0, "jill", 18, true))
    }
  }

  "LEFT/RIGHT Join" should {
    "work with equality expressions" in {
      val join = Join($"input0.name" === $"input1.name", LEFT)
      (grid1, grid2) --> join
      assertOutput(join, 0, ("john", 1, 25.0, "john", 25, true),
        ("jane", 1, 46.0, "jane", 35, false), ("jake", 4, 62.0, null, null, null))
    }
    "work with inequality expressions" in {
      val join = Join($"score" < $"age", LEFT)
      (grid1, grid2) --> join

      assertOutput(join, 0, ("john", 1, 25.0, "jane", 35, false),
        ("jane", 1, 46.0, null, null, null), ("jake", 4, 62.0, null, null, null))
    }
  }

  "OUTER Join" should {
    "work with equality expressions" in {
      val join = Join($"input0.name" === $"input1.name", OUTER)
      (grid1, grid2) --> join
      assertOutput(join, 0, ("john", 1, 25.0, "john", 25, true),
        ("jane", 1, 46.0, "jane", 35, false), ("jake", 4, 62.0, null, null, null),
        (null, null, null, "jill", 18, true))
    }
  }

  "Join" should {
    "save to/load from xml" in {
      import com.ignition.util.XmlUtils._

      val j1 = Join()
      j1.toXml must ==/(<join type="inner"></join>)
      Join.fromXml(j1.toXml) === j1

      val j2 = Join($"input0.name" === $"input1.name")
      j2.toXml must ==/(<join type="inner"><condition>(input0.name = input1.name)</condition></join>)
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
      j1.toJson === ("tag" -> "join") ~ ("type" -> "inner") ~ ("condition" -> jNone)
      Join.fromJson(j1.toJson) === j1

      val j2 = Join($"input0.name" === $"input1.name")
      j2.toJson === ("tag" -> "join") ~ ("type" -> "inner") ~ ("condition" -> "(input0.name = input1.name)")
      Join.fromJson(j2.toJson).condition.map(_.toString) === j2.condition.map(_.toString)

      val j3 = Join($"a" < $"b", OUTER)
      j3.toJson === ("tag" -> "join") ~ ("type" -> "outer") ~ ("condition" -> "(a < b)")
      Join.fromJson(j3.toJson).condition.map(_.toString) === j3.condition.map(_.toString)
    }
    "be unserializable" in assertUnserializable(Join($"input0.item" === $"input1.item"))
  }
}