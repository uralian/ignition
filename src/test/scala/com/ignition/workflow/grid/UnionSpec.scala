package com.ignition.workflow.grid

import org.junit.runner.RunWith
import org.slf4j.LoggerFactory
import org.specs2.matcher.XmlMatchers
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import com.ignition.SparkTestHelper
import com.ignition.data.{ boolean, columnInfo2metaData, int, string }
import com.ignition.workflow.WorkflowException
import com.ignition.workflow.rdd.grid.Union
import com.ignition.workflow.rdd.grid.input.DataGridInput

@RunWith(classOf[JUnitRunner])
class UnionSpec extends Specification with XmlMatchers with SparkTestHelper {
  val log = LoggerFactory.getLogger(getClass)

  val meta = string("a") ~ int("b") ~ boolean("c")

  val grid1 = DataGridInput(meta).
    addRow("xyz", 1, true).
    addRow("ccc", 0, true)

  val grid2 = DataGridInput(meta).
    addRow("kmk", 5, false).
    addRow("ppp", 1, true).
    addRow("abc", 0, true)

  val grid3 = DataGridInput(meta).
    addRow("zzz", 5, false).
    addRow("xyz", 1, true)

  "Union" should {
    "merge identical rows" in {
      val union = Union()
      union.connectFrom(grid1)
      union.connectFrom(grid2)
      union.connectFrom(grid3)

      union.outMetaData === Some(meta)
      union.output.collect.toSet === Set(
        meta.row("xyz", 1, true), meta.row("ccc", 0, true),
        meta.row("kmk", 5, false), meta.row("ppp", 1, true), meta.row("abc", 0, true),
        meta.row("zzz", 5, false), meta.row("xyz", 1, true))
    }
    "fail for unmatching metadata" in {
      val metaX = string("a") ~ int("b")
      val gridX = DataGridInput(metaX).addRow("aaa", 666)

      val union = Union()
      union.connectFrom(grid1)
      union.connectFrom(grid2)
      union.connectFrom(gridX)

      union.outMetaData must throwA[WorkflowException]
      union.output.collect must throwA[WorkflowException]
    }
    "fail for unconnected inputs" in {
      val union = Union()
      union.outMetaData must beNone
      union.output.collect must throwA[WorkflowException]
    }
  }
}