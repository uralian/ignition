package com.ignition.frame

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.types.{ RichStructType, double, fieldToRichStruct, int, string }

import ReduceOp._

@RunWith(classOf[JUnitRunner])
class ReduceSpec extends FrameFlowSpecification {

  import ReduceOp._

  val schema = string("name") ~ int("item") ~ double("score")
  val grid = DataGrid(schema) rows (
    ("john", 1, 65.0), ("john", 3, 78.0), ("jane", 2, 85.0),
    ("jane", 1, 46.0), ("jake", 4, 62.0), ("john", 3, 95.0))

  "Reduce" should {
    "compute without grouping" in {
      val reduce = Reduce("item" -> SUM, "score" -> MIN, "score" -> MAX)
      grid --> reduce

      assertSchema(int("item_SUM") ~ double("score_MIN") ~ double("score_MAX"), reduce, 0)
      assertOutput(reduce, 0, (14, 46.0, 95.0))
    }
    "compute with grouping" in {
      val reduce = Reduce() % SUM("item") % SUM("score") groupBy ("name")
      grid --> reduce

      assertSchema(string("name") ~ int("item_SUM") ~ double("score_SUM"), reduce, 0)
      assertOutput(reduce, 0, ("john", 7, 238.0), ("jane", 3, 131.0), ("jake", 4, 62.0))
    }
    "save to/load from xml" in {
      val r1 = Reduce("item" -> SUM, "score" -> MIN)
      r1.toXml must ==/(
        <reduce>
          <aggregate>
            <field name="item" type="SUM"/><field name="score" type="MIN"/>
          </aggregate>
        </reduce>)
      Reduce.fromXml(r1.toXml) === r1

      val r2 = Reduce() % SUM("item") % SUM("score") groupBy ("name")
      r2.toXml must ==/(
        <reduce>
          <aggregate>
            <field name="item" type="SUM"/><field name="score" type="SUM"/>
          </aggregate>
          <group-by>
            <field name="name"/>
          </group-by>
        </reduce>)
      Reduce.fromXml(r2.toXml) === r2
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._

      val r1 = Reduce("item" -> SUM, "score" -> MIN)
      r1.toJson === ("tag" -> "reduce") ~
        ("groupBy" -> (None: Option[String])) ~
        ("aggregate" -> List("item" -> "SUM", "score" -> "MIN"))
      Reduce.fromJson(r1.toJson) === r1

      val r2 = Reduce() % SUM("item") % SUM("score") groupBy ("name")
      r2.toJson === ("tag" -> "reduce") ~
        ("groupBy" -> List("name")) ~
        ("aggregate" -> List("item" -> "SUM", "score" -> "SUM"))
        Reduce.fromJson(r2.toJson) === r2
    }
    "be unserializable" in assertUnserializable(Reduce("item" -> SUM, "score" -> MIN))
  }
}