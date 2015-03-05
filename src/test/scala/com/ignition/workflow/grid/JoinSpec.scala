package com.ignition.workflow.grid

import org.junit.runner.RunWith
import org.specs2.matcher.XmlMatchers
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import com.ignition.SparkTestHelper
import com.ignition.data.{ DefaultRowMetaData, boolean, columnInfo2metaData, double, int, string }
import com.ignition.workflow.WorkflowException
import com.ignition.workflow.rdd.grid.input.DataGridInput
import com.ignition.workflow.rdd.grid.pair.{ Join, JoinType }

@RunWith(classOf[JUnitRunner])
class JoinSpec extends Specification with XmlMatchers with SparkTestHelper {

  val grid1 = DataGridInput(string("a") ~ int("b") ~ double("c") ~ boolean("d"))
    .addRow("111", 1, 2.5, true)
    .addRow("222", 4, 3.0, false)
    .addRow("111", 2, 1.5, true)
    .addRow("333", 5, 3.3, true)

  val grid2 = DataGridInput(string("q") ~ int("b") ~ double("w") ~ string("a"))
    .addRow("xxx", 2, 0.5, "111")
    .addRow("yyy", 3, 1.3, "111")
    .addRow("zzz", 5, 2.4, "333")

  val meta = DefaultRowMetaData() ~~ grid1.meta ~ string("q") ~ double("w")

  "Join" should {
    "perform inner join" in {
      val join = Join(Set("a", "b"), JoinType.INNER)
      join.connect1From(grid1)
      join.connect2From(grid2)
      join.outMetaData === Some(meta)
      join.output.collect.toSet === Set(
        meta.row("111", 2, 1.5, true, "xxx", 0.5),
        meta.row("333", 5, 3.3, true, "zzz", 2.4))
    }
    "perform left outer join" in {
      val join = Join(Set("a", "b"), JoinType.LEFT_OUTER)
      join.connect1From(grid1)
      join.connect2From(grid2)
      join.outMetaData === Some(meta)
      join.output.collect.toSet === Set(
        meta.row("111", 1, 2.5, true, null, null),
        meta.row("222", 4, 3.0, false, null, null),
        meta.row("111", 2, 1.5, true, "xxx", 0.5),
        meta.row("333", 5, 3.3, true, "zzz", 2.4))
    }
    "perform right outer join" in {
      val join = Join(Set("a", "b"), JoinType.RIGHT_OUTER)
      join.connect1From(grid1)
      join.connect2From(grid2)
      join.outMetaData === Some(meta)
      join.output.collect.toSet === Set(
        meta.row("111", 3, null, null, "yyy", 1.3),
        meta.row("111", 2, 1.5, true, "xxx", 0.5),
        meta.row("333", 5, 3.3, true, "zzz", 2.4))
    }
    "perform full outer join" in {
      val join = Join(Set("a", "b"), JoinType.FULL_OUTER)
      join.connect1From(grid1)
      join.connect2From(grid2)
      join.outMetaData === Some(meta)
      join.output.collect.toSet === Set(
        meta.row("111", 2, 1.5, true, "xxx", 0.5),
        meta.row("333", 5, 3.3, true, "zzz", 2.4),
        meta.row("111", 1, 2.5, true, null, null),
        meta.row("222", 4, 3.0, false, null, null),
        meta.row("111", 3, null, null, "yyy", 1.3))
    }
    "fail for empty key set" in {
      Join() must throwA[AssertionError]
    }
    "fail for disconnected inputs" in {
      Join("a", "b").output must throwA[WorkflowException]
    }
    "fail for missing key columns" in {
      val join = Join("o")
      join.connect1From(grid1)
      join.connect2From(grid2)
      join.outMetaData must throwA[AssertionError]
    }
    "save to xml" in {
      Join("a", "b").toXml must ==/(<join type="inner"><key>a</key><key>b</key></join>)
      Join(Set("a"), JoinType.LEFT_OUTER).toXml must ==/(<join type="left"><key>a</key></join>)
      Join(Set("a"), JoinType.FULL_OUTER).toXml must ==/(<join type="full"><key>a</key></join>)
      Join(Set("b"), JoinType.RIGHT_OUTER, Some(8)).toXml must ==/(
        <join type="right" partitions="8"><key>b</key></join>)
    }
    "load from xml" in {
      Join.fromXml(<join type="right"><key>a</key><key>b</key></join>) === Join(Set("a", "b"), JoinType.RIGHT_OUTER)
      Join.fromXml(<join type="left" partitions="4"><key>a</key></join>) === Join(Set("a"), JoinType.LEFT_OUTER, Some(4))
    }
  }
}