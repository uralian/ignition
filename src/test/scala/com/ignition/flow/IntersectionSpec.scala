package com.ignition.flow

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import com.ignition.types._
import com.ignition.ExecutionException

@RunWith(classOf[JUnitRunner])
class IntersectionSpec extends FlowSpecification {

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
    "be unserializable" in assertUnserializable(Intersection())
  }
}