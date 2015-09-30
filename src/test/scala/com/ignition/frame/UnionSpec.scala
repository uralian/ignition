package com.ignition.frame

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.{ ExecutionException, RichProduct }
import com.ignition.types.{ RichStructType, boolean, fieldToRichStruct, int, string }

@RunWith(classOf[JUnitRunner])
class UnionSpec extends FrameFlowSpecification {

  val schema = string("a") ~ int("b") ~ boolean("c")

  val grid1 = DataGrid(schema)
    .addRow("xyz", 1, true)
    .addRow("ccc", 0, true)

  val grid2 = DataGrid(schema)
    .addRow("kmk", 5, false)
    .addRow("ppp", 1, true)
    .addRow("abc", 0, true)

  val grid3 = DataGrid(schema)
    .addRow("zzz", 5, false)
    .addRow("xyz", 1, true)

  "Union" should {
    "merge identical rows" in {
      val union = Union()
      (grid1, grid2, grid3) --> union

      assertSchema(schema, union, 0)
      assertOutput(union, 0,
        Seq("xyz", 1, true), Seq("ccc", 0, true), Seq("kmk", 5, false), Seq("ppp", 1, true),
        Seq("abc", 0, true), Seq("zzz", 5, false), Seq("xyz", 1, true))
    }
    "fail for unmatching metadata" in {
      val metaX = string("a") ~ int("b")
      val gridX = DataGrid(metaX).addRow("aaa", 666)

      val union = Union()
      (grid1, grid2, gridX) --> union

      union.outSchema must throwA[ExecutionException]
      union.output.collect must throwA[ExecutionException]
    }
    "fail for unconnected inputs" in {
      val union = Union()
      union.outSchema must throwA[ExecutionException]
      union.output.collect must throwA[ExecutionException]
    }
    "save to/load from xml" in {
      val union = Union()
      union.toXml must ==/(<union/>)
      Union.fromXml(union.toXml) === union
    }
    "save to/load from json" in {
      import org.json4s.JValue
      import org.json4s.JsonDSL._

      val union = Union()
      union.toJson === (("tag" -> "union"): JValue)
      Union.fromJson(union.toJson) === union
    }
    "be unserializable" in assertUnserializable(Union())
  }
}