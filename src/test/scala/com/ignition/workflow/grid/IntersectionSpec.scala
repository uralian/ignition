package com.ignition.workflow.grid

import org.junit.runner.RunWith
import org.specs2.matcher.XmlMatchers
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import com.ignition.SparkTestHelper
import com.ignition.data.{ columnInfo2metaData, double, int, string }
import com.ignition.workflow.WorkflowException
import com.ignition.workflow.rdd.grid.Intersection
import com.ignition.workflow.rdd.grid.input.DataGridInput

@RunWith(classOf[JUnitRunner])
class IntersectionSpec extends Specification with XmlMatchers with SparkTestHelper {

  "Intersection" should {
    "work for valid inputs" in {
      val meta = int("a") ~ string("b") ~ double("c")
      val grid1 = DataGridInput(meta)
        .addRow(1, "john", 25.5).addRow(2, "jack", 0.33).addRow(3, "jane", 9.0)
        .addRow(5, "jill", 123.45).addRow(8, "josh", -3.5).addRow(13, "jake", 0)
      val grid2 = DataGridInput(meta)
        .addRow(1, "jess", 25.5).addRow(2, "jack", 0.33).addRow(3, "jane", 8.0)
        .addRow(5, "jill", 123.45).addRow(4, "josh", -3.5).addRow(2, "jade", 0)

      val isect = Intersection()
      isect.connect1From(grid1)
      isect.connect2From(grid2)

      isect.output.collect.toSet === Set(meta.row(2, "jack", 0.33), meta.row(5, "jill", 123.45))
      isect.outMetaData === Some(meta)
    }
    "fail for disconnected inputs" in {
      val isect = Intersection()
      isect.output must throwA[WorkflowException]
      isect.outMetaData must beNone
    }
    "fail for one disconnected input" in {
      val meta = int("a") ~ string("b") ~ double("c")
      val grid = DataGridInput(meta)
      val isect = new Intersection()
      isect.connect1From(grid)
      isect.output must throwA[WorkflowException]
      isect.outMetaData must beNone
    }
    "fail for inputs with different metadata" in {
      val grid1 = DataGridInput(int("a") ~ string("b")).addRow(1, "john")
      val grid2 = DataGridInput(string("a") ~ int("b")).addRow("john", 1)

      val isect = Intersection()
      isect.connect1From(grid1)
      isect.connect2From(grid2)

      isect.output must throwA[WorkflowException]
      isect.outMetaData must throwA[WorkflowException]
    }
    "save to xml" in {
      Intersection().toXml must ==/(<intersection/>)
      Intersection(Some(4)).toXml must ==/(<intersection partitions="4"/>)
    }
    "load from xml" in {
      Intersection.fromXml(<intersection/>) === Intersection()
      Intersection.fromXml(<intersection partitions="4"/>) === Intersection(Some(4))
    }
  }
}