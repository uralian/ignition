package com.ignition.flow

import org.apache.spark.sql.Row
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.types._

@RunWith(classOf[JUnitRunner])
class ReduceSpec extends FlowSpecification {
  import ReduceOp._

  val schema = string("name") ~ int("item") ~ double("score")
  val grid = DataGrid(schema) rows (
    ("john", 1, 65.0), ("john", 3, 78.0), ("jane", 2, 85.0),
    ("jane", 1, 46.0), ("jake", 4, 62.0), ("john", 3, 95.0))

  "Reduce" should {
    "compute without grouping" in {
      val reduce = Reduce("item" -> SUM, "score" -> MIN, "score" -> MAX)
      grid --> reduce

      assertSchema(int("item_SUM") ~ double("score_MIN") ~ double("score_MAX"), reduce, 0)
      assertOutput(reduce, 0, (14, 46.0, 95.0))
    }
    "compute with grouping" in {
      val reduce = Reduce("item" -> SUM, "score" -> SUM) groupBy ("name")
      grid --> reduce

      assertSchema(string("name") ~ int("item_SUM") ~ double("score_SUM"), reduce, 0)
      assertOutput(reduce, 0, ("john", 7, 238.0), ("jane", 3, 131.0), ("jake", 4, 62.0))
    }
  }
}