package com.ignition.frame

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.types.{ RichStructType, double, fieldToRichStruct, int, long, string }

@RunWith(classOf[JUnitRunner])
class BasicStatsSpec extends FrameFlowSpecification {
  import BasicAggregator._

  val schema = string("name") ~ int("item") ~ int("score")
  val grid = DataGrid(schema) rows (
    ("john", 1, 65), ("john", 3, 78), ("jane", 2, 85), ("jake", 2, 94), ("jake", 1, 70),
    ("jane", 1, 46), ("jake", 4, 62), ("john", 3, 95), ("jane", 3, 50), ("jane", 1, 80))

  "BasicStats" should {
    "aggregate without grouping fields" in {
      val stats = BasicStats() % ("score", MIN, MAX, AVG)
      grid --> stats

      assertSchema(int("score_min") ~ int("score_max") ~ double("score_avg"), stats, 0)
      assertOutput(stats, 0, (46, 95, 72.5))
    }
    "aggregate with one grouping field" in {
      val stats = BasicStats() % COUNT("item") % SUM("score") groupBy "name"
      grid --> stats

      assertSchema(string("name") ~ long("item_cnt", false) ~ long("score_sum"), stats, 0)
      assertOutput(stats, 0, ("john", 3, 238), ("jane", 4, 261), ("jake", 3, 226))
    }
    "aggregate with two grouping fields" in {
      val stats = BasicStats("score" -> AVG) groupBy ("name", "item")
      grid --> stats

      assertSchema(string("name") ~ int("item") ~ double("score_avg"), stats, 0)
      assertOutput(stats, 0,
        ("john", 1, 65.0), ("john", 3, 86.5), ("jane", 2, 85.0), ("jake", 2, 94.0),
        ("jake", 1, 70.0), ("jane", 1, 63.0), ("jake", 4, 62.0), ("jane", 3, 50.0))
    }
    "save to/load from xml" in {
      val s1 = BasicStats() % ("score", MIN, MAX, AVG)
      s1.toXml must ==/(
        <basic-stats>
          <aggregate>
            <field name="score" type="MIN"/><field name="score" type="MAX"/><field name="score" type="AVG"/>
          </aggregate>
        </basic-stats>)
      BasicStats.fromXml(s1.toXml) === s1

      val s2 = BasicStats("score" -> AVG) groupBy ("name", "item")
      s2.toXml must ==/(
        <basic-stats>
          <aggregate>
            <field name="score" type="AVG"/>
          </aggregate>
          <group-by>
            <field name="name"/><field name="item"/>
          </group-by>
        </basic-stats>)
      BasicStats.fromXml(s2.toXml) === s2
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._

      val s1 = BasicStats() % ("score", MIN, MAX, AVG)
      s1.toJson === ("tag" -> "basic-stats") ~
        ("groupBy" -> (None: Option[String])) ~
        ("aggregate" -> List(("score" -> "MIN"), ("score" -> "MAX"), ("score" -> "AVG")))
      BasicStats.fromJson(s1.toJson) === s1

      val s2 = BasicStats("score" -> AVG) groupBy ("name", "item")
      s2.toJson === ("tag" -> "basic-stats") ~
        ("groupBy" -> List("name", "item")) ~
        ("aggregate" -> List(("score" -> "AVG")))
      BasicStats.fromJson(s2.toJson) === s2
    }
    "be unserializable" in assertUnserializable(BasicStats())
  }
}