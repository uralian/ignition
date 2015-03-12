package com.ignition.workflow.grid

import org.apache.spark.util.StatCounter
import org.junit.runner.RunWith
import org.specs2.matcher.XmlMatchers
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import com.ignition.SparkTestHelper
import com.ignition.data.{ DataRow, RowMetaData, boolean, columnInfo2metaData, double, int, string }
import com.ignition.workflow.rdd.grid.input.DataGridInput
import com.ignition.workflow.rdd.grid.pair.{ FieldListAggregator, RowAggregator }
import com.ignition.workflow.rdd.grid.pair.Aggregate
import com.ignition.workflow.rdd.grid.pair.Aggregators.{ BasicStats, MkString }

@RunWith(classOf[JUnitRunner])
class AggregateSpec extends Specification with XmlMatchers with SparkTestHelper {

  val grid = DataGridInput(string("a") ~ int("b") ~ double("c") ~ boolean("d"))
    .addRow("111", 1, 2.5, true)
    .addRow("222", 2, 3.0, false)
    .addRow("111", 2, 1.5, false)
    .addRow("333", 5, 3.3, true)
    .addRow("111", 2, 3.7, false)
    .addRow("222", 1, 1.7, false)
    .addRow("222", 2, 4.2, true)

  "MkString" should {
    val aggr = MkString("a", ":", "<", ">")
    "merge strings" in aggr.seqOp("abc:def", "xyz") === "abc:def:xyz"
    "combine strings" in aggr.combOp("abc", "def") === "abc:def"
    "convert to data" in aggr.invert("abc:def") === Vector("<abc:def>")
    "provide metadata" in aggr.outMetaData === (string("a"): RowMetaData)
    "save to xml" in {
      aggr.toXml must ==/(
        <mk-string name="a">
          <separator>:</separator><prefix>{ "<" }</prefix><suffix>{ ">" }</suffix>
        </mk-string>)
    }
    "load from xml" in {
      val xml =
        <mk-string name="b">
          <separator>,</separator><prefix/><suffix/>
        </mk-string>
      MkString.fromXml(xml) === MkString("b")
    }
  }

  "BasicStats" should {
    val aggr = BasicStats[Int]("a")
    "merge data into total" in eq(aggr.seqOp(StatCounter(1, 2), 5), StatCounter(1, 2, 5))
    "combine totals" in eq(aggr.combOp(StatCounter(1, 2), StatCounter(3, 4)), StatCounter(1, 2, 3, 4))
    "convert to data" in aggr.invert(StatCounter(1, 2, 3, 5)) === Vector(4, 1, 5, 11, 2.75)
    "provide metadata" in aggr.outMetaData ===
      int("a_cnt") ~ int("a_min") ~ int("a_max") ~ int("a_sum") ~ double("a_avg")
    "save to xml" in { aggr.toXml must ==/(<basic-stats name="a" type="int"/>) }
    "load from xml" in {
      val xml = <basic-stats name="b" type="double"/>
      BasicStats.fromXml(xml) === BasicStats[Double]("b")
    }
  }

  "FieldListAggregator" should {
    val meta = string("a") ~ int("b")
    val outMeta = string("a") ~ int("b_cnt") ~ int("b_min") ~ int("b_max") ~ int("b_sum") ~ double("b_avg")
    val aggr = FieldListAggregator(MkString("a"), BasicStats[Int]("b"))

    "convert data row" in aggr.convert(meta.row("abc", 5)) === List("abc", 5)
    "compute zero" in {
      aggr.zero.toList(0) === ""
      aggr.zero.toList(1).asInstanceOf[StatCounter].count === 0
    }
    "merge data into total" in {
      val total = aggr.seqOp(List("abc", StatCounter(2)), meta.row("xyz", 3))
      total.toList(0) === "abc,xyz"
      eq(total.toList(1).asInstanceOf[StatCounter], StatCounter(2).merge(3))
    }
    "combine totals" in {
      val total = aggr.combOp(List("a,b", StatCounter(2, 3)), List("c,d,e", StatCounter(1, 3, 5)))
      total.toList(0) === "a,b,c,d,e"
      eq(total.toList(1).asInstanceOf[StatCounter], StatCounter(2, 3, 1, 3, 5))
    }
    "invert data to data row" in {
      val row = aggr.invert(List("abc,def", StatCounter(1, 2, 3, 4)))
      row.getString("a") === "abc,def"
      row.getInt("b_cnt") === 4
      row.getInt("b_min") === 1
      row.getInt("b_max") === 4
      row.getInt("b_sum") === 10
      row.getDouble("b_avg") === (1 + 2 + 3 + 4) / 4.0
    }
    "provide metadata" in aggr.outMetaData === outMeta
    "save to xml" in {
      aggr.toXml must ==/(
        <field-list-aggregator>
          <mk-string name="a"><separator>,</separator><prefix/><suffix/></mk-string>
          <basic-stats name="b" type="int"/>
        </field-list-aggregator>)
    }
    "load from xml" in {
      val xml =
        <field-list-aggregator>
          <mk-string name="a"><separator>:</separator><prefix>+</prefix><suffix>-</suffix></mk-string>
          <basic-stats name="b" type="int"/>
        </field-list-aggregator>
      FieldListAggregator.fromXml(xml) === FieldListAggregator(MkString("a", ":", "+", "-"), BasicStats[Int]("b"))
    }
  }

  "Aggregate" should {
    val ra = FieldListAggregator(BasicStats[Double]("c"))

    "fail for empty key set" in {
      new Aggregate(Nil, ra) {} must throwA[AssertionError]
    }
    "fail for aggregator containing a key column" in {
      val emptyAggr = new RowAggregator[Int] {
        def convert(row: DataRow) = 0
        def zero = 0
        def seqOp(value: Int, row: DataRow) = 0
        def combOp(value1: Int, value2: Int) = 0
        def invert(value: Int) = outMetaData.row(0)
        def outMetaData = int("b")
        def toXml = ???
      }
      new Aggregate(List("b"), emptyAggr) {} must throwA[AssertionError]
    }
    "perform aggregation" in {
      val aggr = Aggregate(List("a", "b"), ra)
      grid.connectTo(aggr)

      val meta = string("a") ~ int("b") ~ int("c_cnt") ~ double("c_min") ~ double("c_max") ~ double("c_sum") ~ double("c_avg")
      aggr.outMetaData === Some(meta)
      aggr.output.collect.toSet === Set(
        meta.row("333", 5, 1, 3.3, 3.3, 3.3, 3.3),
        meta.row("111", 1, 1, 2.5, 2.5, 2.5, 2.5),
        meta.row("111", 2, 2, 1.5, 3.7, 5.2, 2.6),
        meta.row("222", 2, 2, 3.0, 4.2, 7.2, 3.6),
        meta.row("222", 1, 1, 1.7, 1.7, 1.7, 1.7))
    }
    "save to xml" in {
      val aggr = Aggregate(List("a", "b"), ra)
      aggr.toXml must ==/(
        <aggregate>
          <keys><key>a</key><key>b</key></keys>
          <field-list-aggregator>
            <basic-stats name="c" type="double"/>
          </field-list-aggregator>
        </aggregate>)
    }
    "load from xml" in {
      val xml =
        <aggregate>
          <keys><key>a</key><key>b</key></keys>
          <field-list-aggregator>
            <basic-stats name="c" type="double"/>
          </field-list-aggregator>
        </aggregate>
      Aggregate.fromXml(xml) === Aggregate(List("a", "b"), ra)
    }
  }

  def eq(sc1: StatCounter, sc2: StatCounter) = {
    sc1.count === sc2.count
    sc1.max === sc2.max
    sc1.min === sc2.min
    sc1.mean === sc2.mean
    sc1.sum === sc2.sum
    sc1.variance === sc2.variance
  }
}