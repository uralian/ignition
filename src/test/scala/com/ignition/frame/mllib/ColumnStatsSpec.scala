package com.ignition.frame.mllib

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.frame.{ DataGrid, FrameFlowSpecification }
import com.ignition.types.{ RichStructType, double, fieldToRichStruct, int, long, string }

@RunWith(classOf[JUnitRunner])
class ColumnStatsSpec extends FrameFlowSpecification {

  val schema = string("name") ~ int("item") ~ int("score")
  val grid = DataGrid(schema) rows (
    ("john", 1, 65), ("john", 3, 78), ("jane", 2, 85), ("jake", 2, 94), ("jake", 1, 70),
    ("jane", 1, 46), ("jake", 4, 62), ("john", 3, 95), ("jane", 3, 50), ("jane", 1, 80))

  "ColumnStats" should {
    "compute stats without grouping" in {
      val stats = ColumnStats() % "score" % "item"
      grid --> stats

      assertSchema(long("count") ~
        double("score_max") ~ double("score_min") ~ double("score_mean") ~
        double("score_non0") ~ double("score_variance") ~
        double("score_normL1") ~ double("score_normL2") ~
        double("item_max") ~ double("item_min") ~ double("item_mean") ~
        double("item_non0") ~ double("item_variance") ~
        double("item_normL1") ~ double("item_normL2"), stats, 0)

      val out = stats.output.collect
      out.size === 1

      val row = out.head
      row.getLong(0) === 10
      row.getDouble(1) === 95.0
      row.getDouble(2) === 46.0
      row.getDouble(3) === 72.5
    }
    "compute stats with grouping" in {
      val stats = ColumnStats() add ("score", "item") groupBy "name"
      grid --> stats

      assertSchema(string("name") ~ long("count") ~
        double("score_max") ~ double("score_min") ~ double("score_mean") ~
        double("score_non0") ~ double("score_variance") ~
        double("score_normL1") ~ double("score_normL2") ~
        double("item_max") ~ double("item_min") ~ double("item_mean") ~
        double("item_non0") ~ double("item_variance") ~
        double("item_normL1") ~ double("item_normL2"), stats, 0)

      val out = stats.output.collect
      out.size === 3

      val john = out.filter(_.getString(0) == "john")
      john.size === 1
      john.head.getLong(1) === 3

      val jake = out.filter(_.getString(0) == "jake")
      jake.size === 1
      jake.head.getLong(1) === 3

      val jane = out.filter(_.getString(0) == "jane")
      jane.size === 1
      jane.head.getLong(1) === 4
    }
    "save to/load from xml" in {
      val s1 = ColumnStats() % "score" % "item"
      s1.toXml must ==/(
        <column-stats>
          <aggregate>
            <field name="score"/><field name="item"/>
          </aggregate>
        </column-stats>)
      ColumnStats.fromXml(s1.toXml) === s1

      val s2 = ColumnStats() add ("score", "item") groupBy "name"
      s2.toXml must ==/(
        <column-stats>
          <aggregate>
            <field name="score"/><field name="item"/>
          </aggregate>
          <group-by>
            <field name="name"/>
          </group-by>
        </column-stats>)
      ColumnStats.fromXml(s2.toXml) === s2
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._

      val s1 = ColumnStats() % "score" % "item"
      s1.toJson === ("tag" -> "column-stats") ~ ("aggregate" -> List("score", "item")) ~ ("groupBy" -> jNone)
      ColumnStats.fromJson(s1.toJson) === s1

      val s2 = ColumnStats() add ("score", "item") groupBy "name"
      s2.toJson === ("tag" -> "column-stats") ~ ("aggregate" -> List("score", "item")) ~ ("groupBy" -> List("name"))
      ColumnStats.fromJson(s2.toJson) === s2
    }
    "be unserializable" in assertUnserializable(ColumnStats() add ("score", "item") groupBy "name")
  }
}