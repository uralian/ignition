package com.ignition.frame

import org.apache.spark.sql.Row
import org.json4s.JArray
import org.json4s.JsonDSL.{ int2jvalue, jobject2assoc, pair2Assoc, pair2jvalue, string2jvalue }
import org.json4s.jvalue2monadic
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.CSource2
import com.ignition.types.{ RichStructType, fieldToRichStruct, fieldToStructType, int, string }
import com.ignition.util.JsonUtils.RichJValue

@RunWith(classOf[JUnitRunner])
class SubFlowSpec extends FrameFlowSpecification {
  sequential

  "FrameSubProducer" should {
    val flow = FrameSubProducer {
      val grid = DataGrid(string("name")) rows (Tuple1("john"), Tuple1("jane"), Tuple1("jake"))
      val filter = Filter("name rlike 'ja.e'")
      grid --> filter
      filter.out(0)
    }
    "yield the output" in {
      assertSchema(string("name"), flow, 0)
      assertOutput(flow, 0, Row("jane"), Row("jake"))
    }
    "save to/load from xml" in {
      flow.toXml must ==/(
        <subflow>
          <steps>
            <filter id="filter0">
              <condition>name rlike 'ja.e'</condition>
            </filter>
            <datagrid id="datagrid0">
              <schema>
                <field name="name" type="string" nullable="true"/>
              </schema>
              <rows>
                <row><item>john</item></row>
                <row><item>jane</item></row>
                <row><item>jake</item></row>
              </rows>
            </datagrid>
          </steps>
          <connections><connect src="datagrid0" srcPort="0" tgt="filter0" tgtPort="0"/></connections>
          <in-points/>
          <out-points><step id="filter0" port="0"/></out-points>
        </subflow>)
      val flow2 = FrameSubFlow.fromXml(flow.toXml)
      flow.steps === flow2.steps
      flow.connections === flow2.connections
      assertSchema(string("name"), flow2, 0)
      assertOutput(flow2, 0, Row("jane"), Row("jake"))
    }
    "save to/load from json" in {
      val json = flow.toJson
      (json \ "steps" asArray).length === 2
      (json \ "steps").find(x => (x \ "tag" asString) == Filter.tag) must beSome
      (json \ "steps").find(x => (x \ "tag" asString) == DataGrid.tag) must beSome
      (json \ "connections") === JArray(List(
        ("src" -> "datagrid0") ~ ("srcPort" -> 0) ~ ("tgt" -> "filter0") ~ ("tgtPort" -> 0)))
      val flow2 = FrameSubFlow.fromJson(json)
      flow.steps === flow2.steps
      flow.connections === flow2.connections
      assertSchema(string("name"), flow2, 0)
      assertOutput(flow2, 0, Row("jane"), Row("jake"))
    }
    "be unserializable" in assertUnserializable(flow)
  }

  "FrameSubTransformer" should {
    val flow = FrameSubTransformer {
      import ReduceOp._

      val select = SelectValues() rename ("name" -> "fname")
      val reduce = Reduce("fname" -> CONCAT)
      select --> reduce
      (select, reduce)
    }
    val grid = DataGrid(string("name")) rows (Tuple1("john"), Tuple1("jane"), Tuple1("jake"))
    "yield the output" in {
      grid --> flow
      assertSchema(string("fname_CONCAT"), flow, 0)
      assertOutput(flow, 0, Row("johnjanejake"))
    }
    "save to/load from xml" in {
      flow.toXml must ==/(
        <subflow>
          <steps>
            <reduce id="reduce0">
              <aggregate>
                <field name="fname" type="CONCAT"/>
              </aggregate>
            </reduce>
            <select-values id="select-values0">
              <rename><field oldName="name" newName="fname"/></rename>
            </select-values>
          </steps>
          <connections>
            <connect src="select-values0" srcPort="0" tgt="reduce0" tgtPort="0"/>
          </connections>
          <in-points>
            <step id="select-values0" port="0"/>
          </in-points>
          <out-points>
            <step id="reduce0" port="0"/>
          </out-points>
        </subflow>)
      val flow2 = FrameSubFlow.fromXml(flow.toXml).asInstanceOf[FrameSubTransformer]
      flow.steps === flow2.steps
      flow.connections === flow2.connections
      grid --> flow2
      assertSchema(string("fname_CONCAT"), flow2, 0)
      assertOutput(flow2, 0, Row("johnjanejake"))
    }
    "save to/load from json" in {
      val json = flow.toJson
      (json \ "steps" asArray).length === 2
      (json \ "steps").find(x => (x \ "tag" asString) == SelectValues.tag) must beSome
      (json \ "steps").find(x => (x \ "tag" asString) == Reduce.tag) must beSome
      (json \ "connections") === JArray(List(
        ("src" -> "select-values0") ~ ("srcPort" -> 0) ~ ("tgt" -> "reduce0") ~ ("tgtPort" -> 0)))
      val flow2 = FrameSubFlow.fromJson(flow.toJson).asInstanceOf[FrameSubTransformer]
      flow.steps === flow2.steps
      flow.connections === flow2.connections
      grid --> flow2
      assertSchema(string("fname_CONCAT"), flow2, 0)
      assertOutput(flow2, 0, Row("johnjanejake"))
    }
    "be unserializable" in assertUnserializable(flow)
  }

  "FrameSubSplitter" should {
    val flow = FrameSubSplitter {
      val select = SelectValues() retain ("name")
      val filter = Filter("name RLIKE 'jo.*'")
      select --> filter
      (select, filter.out)
    }
    val grid = DataGrid(string("name")) rows (Tuple1("john"), Tuple1("jane"), Tuple1("jake"))
    "yield the output" in {
      grid --> flow
      assertSchema(string("name"), flow, 0)
      assertOutput(flow, 0, Row("john"))
      assertOutput(flow, 1, Row("jane"), Row("jake"))
    }
    "save to/load from xml" in {
      flow.toXml must ==/(
        <subflow>
          <steps>
            <filter id="filter0"><condition>name RLIKE 'jo.*'</condition></filter>
            <select-values id="select-values0"><retain><field name="name"/></retain></select-values>
          </steps>
          <connections>
            <connect src="select-values0" srcPort="0" tgt="filter0" tgtPort="0"/>
          </connections>
          <in-points>
            <step id="select-values0" port="0"/>
          </in-points>
          <out-points>
            <step id="filter0" port="0"/><step id="filter0" port="1"/>
          </out-points>
        </subflow>)
      val flow2 = FrameSubFlow.fromXml(flow.toXml).asInstanceOf[FrameSubSplitter]
      flow.steps === flow2.steps
      flow.connections === flow2.connections
      grid --> flow2
      assertSchema(string("name"), flow2, 0)
      assertOutput(flow2, 0, Row("john"))
      assertOutput(flow2, 1, Row("jane"), Row("jake"))
    }
    "save to/load from json" in {
      val json = flow.toJson
      (json \ "steps" asArray).length === 2
      (json \ "steps").find(x => (x \ "tag" asString) == SelectValues.tag) must beSome
      (json \ "steps").find(x => (x \ "tag" asString) == Filter.tag) must beSome
      (json \ "connections") === JArray(List(
        ("src" -> "select-values0") ~ ("srcPort" -> 0) ~ ("tgt" -> "filter0") ~ ("tgtPort" -> 0)))
      val flow2 = FrameSubFlow.fromJson(flow.toJson).asInstanceOf[FrameSubSplitter]
      flow.steps === flow2.steps
      flow.connections === flow2.connections
      grid --> flow2
      assertSchema(string("name"), flow2, 0)
      assertOutput(flow2, 0, Row("john"))
      assertOutput(flow2, 1, Row("jane"), Row("jake"))
    }
    "be unserializable" in assertUnserializable(flow)
  }

  "FrameSubMerger" should {
    val flow = FrameSubMerger {
      val select = SelectValues() retain ("name", "age")
      val sql = SQLQuery("SELECT input0.name, age, weight FROM input0 JOIN input1 ON input0.name=input1.name")
      select --> sql.in(0)
      (Seq(select, sql.in(1)), sql)
    }
    val grid0 = DataGrid(string("name") ~ int("age")) rows (("john", 25), ("jane", 18), ("jake", 42))
    val grid1 = DataGrid(string("name") ~ int("weight")) rows (("john", 165), ("jane", 105), ("jake", 180))
    "yield the output" in {
      (grid0, grid1) --> flow
      assertSchema(string("name") ~ int("age") ~ int("weight"), flow, 0)
      assertOutput(flow, 0, ("john", 25, 165), ("jake", 42, 180), ("jane", 18, 105))
    }
    "save to/load from xml" in {
      flow.toXml must ==/(
        <subflow>
          <steps>
            <sql id="sql0">SELECT input0.name, age, weight FROM input0 JOIN input1 ON input0.name=input1.name</sql>
            <select-values id="select-values0">
              <retain><field name="name"/><field name="age"/></retain>
            </select-values>
          </steps>
          <connections>
            <connect src="select-values0" srcPort="0" tgt="sql0" tgtPort="0"/>
          </connections>
          <in-points>
            <step id="select-values0" port="0"/><step id="sql0" port="1"/>
          </in-points>
          <out-points>
            <step id="sql0" port="0"/>
          </out-points>
        </subflow>)
      val flow2 = FrameSubFlow.fromXml(flow.toXml).asInstanceOf[FrameSubMerger]
      flow.steps === flow2.steps
      flow.connections === flow2.connections
      (grid0, grid1) --> flow2
      assertSchema(string("name") ~ int("age") ~ int("weight"), flow2, 0)
      assertOutput(flow2, 0, ("john", 25, 165), ("jake", 42, 180), ("jane", 18, 105))
    }
    "save to/load from json" in {
      val json = flow.toJson
      (json \ "steps" asArray).length === 2
      (json \ "steps").find(x => (x \ "tag" asString) == SelectValues.tag) must beSome
      (json \ "steps").find(x => (x \ "tag" asString) == SQLQuery.tag) must beSome
      (json \ "connections") === JArray(List(
        ("src" -> "select-values0") ~ ("srcPort" -> 0) ~ ("tgt" -> "sql0") ~ ("tgtPort" -> 0)))
      val flow2 = FrameSubFlow.fromJson(flow.toJson).asInstanceOf[FrameSubMerger]
      flow.steps === flow2.steps
      flow.connections === flow2.connections
      (grid0, grid1) --> flow2
      assertSchema(string("name") ~ int("age") ~ int("weight"), flow2, 0)
      assertOutput(flow2, 0, ("john", 25, 165), ("jake", 42, 180), ("jane", 18, 105))
    }
    "be unserializable" in assertUnserializable(flow)
  }

  "FrameSubModule" should {
    val flow = FrameSubModule {
      val sql = SQLQuery("SELECT input0.name, age, weight FROM input0 JOIN input1 ON input0.name=input1.name")
      val filter = Filter("name RLIKE 'jo.*'")
      sql --> filter
      (sql.in, filter.out)
    }
    val grid0 = DataGrid(string("name") ~ int("age")) rows (("john", 25), ("jane", 18), ("jake", 42))
    val grid1 = DataGrid(string("name") ~ int("weight")) rows (("john", 165), ("jane", 105), ("jake", 180))
    "yield the output" in {
      (grid0, grid1) --> flow
      assertSchema(string("name") ~ int("age") ~ int("weight"), flow, 0)
      assertOutput(flow, 0, ("john", 25, 165))
      assertOutput(flow, 1, ("jake", 42, 180), ("jane", 18, 105))
    }
    "save to/load from xml" in {
      flow.toXml must ==/(
        <subflow>
          <steps>
            <filter id="filter0"><condition>name RLIKE 'jo.*'</condition></filter>
            <sql id="sql0">SELECT input0.name, age, weight FROM input0 JOIN input1 ON input0.name=input1.name</sql>
          </steps>
          <connections>
            <connect src="sql0" srcPort="0" tgt="filter0" tgtPort="0"/>
          </connections>
          <in-points>
            <step id="sql0" port="0"/><step id="sql0" port="1"/>
            <step id="sql0" port="2"/><step id="sql0" port="3"/>
            <step id="sql0" port="4"/><step id="sql0" port="5"/>
            <step id="sql0" port="6"/><step id="sql0" port="7"/>
            <step id="sql0" port="8"/><step id="sql0" port="9"/>
          </in-points>
          <out-points>
            <step id="filter0" port="0"/><step id="filter0" port="1"/>
          </out-points>
        </subflow>)
      val flow2 = FrameSubFlow.fromXml(flow.toXml).asInstanceOf[FrameSubModule]
      flow.steps === flow2.steps
      flow.connections === flow2.connections
      (grid0, grid1) --> flow2
      assertSchema(string("name") ~ int("age") ~ int("weight"), flow2, 0)
      assertOutput(flow2, 0, ("john", 25, 165))
      assertOutput(flow2, 1, ("jake", 42, 180), ("jane", 18, 105))
    }
    "save to/load from json" in {
      val json = flow.toJson
      (json \ "steps" asArray).length === 2
      (json \ "steps").find(x => (x \ "tag" asString) == Filter.tag) must beSome
      (json \ "steps").find(x => (x \ "tag" asString) == SQLQuery.tag) must beSome
      (json \ "connections") === JArray(List(
        ("src" -> "sql0") ~ ("srcPort" -> 0) ~ ("tgt" -> "filter0") ~ ("tgtPort" -> 0)))
      val flow2 = FrameSubFlow.fromJson(flow.toJson).asInstanceOf[FrameSubModule]
      flow.steps === flow2.steps
      flow.connections === flow2.connections
      (grid0, grid1) --> flow2
      assertSchema(string("name") ~ int("age") ~ int("weight"), flow2, 0)
      assertOutput(flow2, 0, ("john", 25, 165))
      assertOutput(flow2, 1, ("jake", 42, 180), ("jane", 18, 105))
    }
    "be unserializable" in assertUnserializable(flow)
  }
}