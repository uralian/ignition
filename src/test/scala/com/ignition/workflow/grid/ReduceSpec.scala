package com.ignition.workflow.grid

import org.junit.runner.RunWith
import org.specs2.matcher.XmlMatchers
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import com.ignition.SparkTestHelper
import com.ignition.data.{ columnInfo2metaData, int, string }
import com.ignition.workflow.WorkflowException
import com.ignition.workflow.rdd.grid.input.DataGridInput
import com.ignition.workflow.rdd.grid.pair.{ Reduce, ReduceOp }

@RunWith(classOf[JUnitRunner])
class ReduceSpec extends Specification with XmlMatchers with SparkTestHelper {

  val meta = string("id") ~ int("hour") ~ string("task") ~ int("points")
  val grid = DataGridInput(meta)
    .addRow("john", 5, "coding", 3)
    .addRow("john", 5, "coding", 1)
    .addRow("john", 1, "design", 2)

    .addRow("jack", 1, "design", 3)
    .addRow("jack", 3, "design", 1)
    .addRow("jack", 3, "design", 4)

    .addRow("jane", 3, "support", 3)
    .addRow("jane", 1, "support", 1)
    .addRow("jane", 2, "coding", 3)

  "Reduce" should {
    "perform SUM reduce" in {
      val reduce = Reduce(Set("id", "task"), Map("points" -> ReduceOp.SUM))
      grid.connectTo(reduce)
      val md = string("id") ~ string("task") ~ int("points")
      reduce.outMetaData === Some(md)
      reduce.output.collect.toSet === Set(
        md.row("john", "coding", 4),
        md.row("john", "design", 2),
        md.row("jack", "design", 8),
        md.row("jane", "support", 4),
        md.row("jane", "coding", 3))
    }
    "perform MIN reduce" in {
      val reduce = Reduce(Set("id"), Map("points" -> ReduceOp.MIN))
      grid.connectTo(reduce)
      val md = string("id") ~ int("points")
      reduce.outMetaData === Some(md)
      reduce.output.collect.toSet === Set(
        md.row("john", 1),
        md.row("jack", 1),
        md.row("jane", 1))
    }
    "perform MAX reduce" in {
      val reduce = Reduce(Set("task"), Map("points" -> ReduceOp.MAX))
      grid.connectTo(reduce)
      val md = string("task") ~ int("points")
      reduce.outMetaData === Some(md)
      reduce.output.collect.toSet === Set(
        md.row("coding", 3),
        md.row("design", 4),
        md.row("support", 3))
    }
    "perform reduce on multiple fields" in {
      val reduce = Reduce(Set("id"), Map("hour" -> ReduceOp.MAX, "points" -> ReduceOp.SUM))
      grid.connectTo(reduce)
      val md = string("id") ~ int("hour") ~ int("points")
      reduce.outMetaData === Some(md)
      reduce.output.collect.toSet === Set(
        md.row("john", 5, 6),
        md.row("jack", 3, 8),
        md.row("jane", 3, 7))
    }
    "fail for empty key set" in {
      Reduce(Set.empty[String], Map("a" -> ReduceOp.SUM)) must throwA[AssertionError]
    }
    "fail for empty ops set" in {
      Reduce(Set("a"), Map.empty[String, ReduceOp.ReduceOp]) must throwA[AssertionError]
    }
    "fail for matching keys and ops" in {
      Reduce("a", "a" -> ReduceOp.SUM) must throwA[AssertionError]
    }
    "fail for disconnected inputs" in {
      Reduce("a", "b" -> ReduceOp.SUM).output must throwA[WorkflowException]
    }
    "fail for missing key columns" in {
      val reduce = Reduce(Set("unknown"), Map("points" -> ReduceOp.MAX))
      grid.connectTo(reduce)
      reduce.outMetaData must throwA[AssertionError]
    }
    "fail for missing data columns" in {
      val reduce = Reduce(Set("task"), Map("unknown" -> ReduceOp.MAX))
      grid.connectTo(reduce)
      reduce.outMetaData must throwA[AssertionError]
    }
    "save to xml" in {
      Reduce("a", "b", "x" -> ReduceOp.SUM, "y" -> ReduceOp.MIN).toXml must ==/(
        <reduce>
          <keys><key>a</key><key>b</key></keys>
          <operations>
            <field name="x" op="SUM"/>
            <field name="y" op="MIN"/>
          </operations>
        </reduce>)
    }
    "load from xml" in {
      val xml = <reduce>
                  <keys><key>a</key><key>b</key></keys>
                  <operations>
                    <field name="x" op="SUM"/>
                    <field name="y" op="MIN"/>
                  </operations>
                </reduce>
      Reduce.fromXml(xml) === Reduce("a", "b", "x" -> ReduceOp.SUM, "y" -> ReduceOp.MIN)
    }
  }
}