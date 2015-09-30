package com.ignition.frame

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.{ ExecutionException, RichProduct }
import com.ignition.types.{ RichStructType, boolean, fieldToRichStruct, int, string }

@RunWith(classOf[JUnitRunner])
class IntersectionSpec extends FrameFlowSpecification {

  val schema = string("a") ~ int("b") ~ boolean("c")

  val grid1 = DataGrid(schema)
    .addRow("xxx", 1, true)
    .addRow("yyy", 0, true)

  val grid2 = DataGrid(schema)
    .addRow("aaa", 0, false)
    .addRow("xxx", 1, true)
    .addRow("xxx", 0, true)
    .addRow("yyy", 0, false)

  val grid3 = DataGrid(schema)
    .addRow("zzz", 2, false)
    .addRow("xxx", 1, true)
    .addRow("yyy", 0, true)

  "Intersection" should {
    "return the input for one input" in {
      val inter = Intersection()
      (grid1) --> inter

      assertSchema(schema, inter, 0)
      assertOutput(inter, 0, Seq("xxx", 1, true), Seq("yyy", 0, true))
    }
    "return the intersection of two inputs (1)" in {
      val inter = Intersection()
      (grid1, grid2) --> inter

      assertSchema(schema, inter, 0)
      assertOutput(inter, 0, Seq("xxx", 1, true))
    }
    "return the intersection of two inputs (2)" in {
      val inter = Intersection()
      (grid1, grid3) --> inter

      assertSchema(schema, inter, 0)
      assertOutput(inter, 0, Seq("xxx", 1, true), Seq("yyy", 0, true))
    }
    "return the intersection of three inputs" in {
      val inter = Intersection()
      (grid1, grid2, grid3) --> inter

      assertSchema(schema, inter, 0)
      assertOutput(inter, 0, Seq("xxx", 1, true))
    }
    "fail for unmatching metadata" in {
      val metaX = string("a") ~ int("b")
      val gridX = DataGrid(metaX).addRow("aaa", 666)

      val inter = Intersection()
      (grid1, grid2, gridX) --> inter

      inter.outSchema must throwA[ExecutionException]
      inter.output.collect must throwA[ExecutionException]
    }
    "fail for unconnected inputs" in {
      val inter = Intersection()
      inter.outSchema must throwA[ExecutionException]
      inter.output.collect must throwA[ExecutionException]
    }
    "save to/load from xml" in {
      val inter = Intersection()
      inter.toXml must ==/(<intersection/>)
      Intersection.fromXml(inter.toXml) === inter
    }
    "save to/load from json" in {
      import org.json4s.JValue
      import org.json4s.JsonDSL._

      val inter = Intersection()
      inter.toJson === (("tag" -> "intersection"): JValue)
      Intersection.fromJson(inter.toJson) === inter
    }
    "be unserializable" in assertUnserializable(Intersection())
  }
}