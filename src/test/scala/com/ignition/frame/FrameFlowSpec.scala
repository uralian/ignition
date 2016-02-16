package com.ignition.frame

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.types.{ RichStructType, double, fieldToRichStruct, int, string }
import com.ignition.util.JsonUtils.RichJValue
import com.ignition.Connection

@RunWith(classOf[JUnitRunner])
class FrameFlowSpec extends FrameFlowSpecification {
  sequential

  import FrameFlow._

  val schema = string("name") ~ int("item") ~ double("score")
  val grid = DataGrid(schema) rows (
    ("john", 1, 65.0), ("john", 3, 78.0), ("jane", 2, 85.0),
    ("jane", 1, 46.0), ("jake", 4, 62.0), ("john", 3, 94.0))
  val sql = SQLQuery("SELECT name, AVG(score) as score FROM input1 GROUP BY name")
  val select = SelectValues() rename ("score" -> "avg_score")
  val stats = BasicStats() groupBy "name" add ("item", BasicAggregator.COUNT)
  val filter = Filter($"avg_score" > 65)

  val flow = FrameFlow {
    grid --> sql.in(1)
    sql --> select --> filter
    grid --> stats
    (filter, stats)
  }

  "FrameFlow" should {
    "enumerate its steps and connections" in {
      flow.steps === Set(grid, sql, select, stats, filter)
      flow.connections === Set(
        Connection(grid, 0, sql, 1),
        Connection(sql, 0, select, 0),
        Connection(select, 0, filter, 0),
        Connection(grid, 0, stats, 0))
    }
    "yield the target outputs" in {
      val results = flow.execute(false).toList
      assertDataFrame(results(0), ("john", 79.0), ("jane", 65.5))
      assertDataFrame(results(1), ("jake", 62.0))
      assertDataFrame(results(2), ("john", 3), ("jake", 1), ("jane", 2))
    }
    "save to/load from xml" in {
      val xml = flow.toXml
      (xml \ "steps" \ "_").length === 5
      (xml \ "steps" \ SelectValues.tag) should not beEmpty;
      (xml \ "steps" \ Filter.tag) should not beEmpty;
      (xml \ "steps" \ BasicStats.tag) should not beEmpty;
      (xml \ "steps" \ SQLQuery.tag) should not beEmpty;
      (xml \ "steps" \ DataGrid.tag) should not beEmpty;
      (xml \ "connections") must ==/(
        <connections>
          <connect src="datagrid0" srcPort="0" tgt="sql0" tgtPort="1"/>
          <connect src="sql0" srcPort="0" tgt="select-values0" tgtPort="0"/>
          <connect src="datagrid0" srcPort="0" tgt="basic-stats0" tgtPort="0"/>
          <connect src="select-values0" srcPort="0" tgt="filter0" tgtPort="0"/>
        </connections>)
      val flow2 = FrameFlow.fromXml(xml)
      flow2.steps === Set(grid, sql, select, stats, filter)
      flow2.connections === Set(
        Connection(grid, 0, sql, 1),
        Connection(sql, 0, select, 0),
        Connection(select, 0, filter, 0),
        Connection(grid, 0, stats, 0))
      val results = flow2.execute(false).toList
      assertDataFrame(results(0), ("john", 79.0), ("jane", 65.5))
      assertDataFrame(results(1), ("jake", 62.0))
      assertDataFrame(results(2), ("john", 3), ("jake", 1), ("jane", 2))
    }
    "save to/load from json" in {
      import org.json4s._
      import org.json4s.JsonDSL._

      val json = flow.toJson
      (json \ "steps" asArray).length === 5
      (json \ "steps").find(x => (x \ "tag" asString) == SelectValues.tag) must beSome
      (json \ "steps").find(x => (x \ "tag" asString) == Filter.tag) must beSome
      (json \ "steps").find(x => (x \ "tag" asString) == BasicStats.tag) must beSome
      (json \ "steps").find(x => (x \ "tag" asString) == SQLQuery.tag) must beSome
      (json \ "steps").find(x => (x \ "tag" asString) == DataGrid.tag) must beSome
      (json \ "connections") === JArray(List(
        ("src" -> "datagrid0") ~ ("srcPort" -> 0) ~ ("tgt" -> "sql0") ~ ("tgtPort" -> 1),
        ("src" -> "select-values0") ~ ("srcPort" -> 0) ~ ("tgt" -> "filter0") ~ ("tgtPort" -> 0),
        ("src" -> "sql0") ~ ("srcPort" -> 0) ~ ("tgt" -> "select-values0") ~ ("tgtPort" -> 0),
        ("src" -> "datagrid0") ~ ("srcPort" -> 0) ~ ("tgt" -> "basic-stats0") ~ ("tgtPort" -> 0)))

      val flow2 = FrameFlow.fromJson(json)
      flow2.steps === Set(grid, sql, select, stats, filter)
      flow2.connections === Set(
        Connection(grid, 0, sql, 1),
        Connection(sql, 0, select, 0),
        Connection(select, 0, filter, 0),
        Connection(grid, 0, stats, 0))
      val results = flow2.execute(false).toList
      assertDataFrame(results(0), ("john", 79.0), ("jane", 65.5))
      assertDataFrame(results(1), ("jake", 62.0))
      assertDataFrame(results(2), ("john", 3), ("jake", 1), ("jane", 2))
    }
  }  
}