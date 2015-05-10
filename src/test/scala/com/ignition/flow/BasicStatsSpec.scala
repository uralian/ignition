package com.ignition.flow

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import com.ignition.types._
import com.eaio.uuid.UUID

@RunWith(classOf[JUnitRunner])
class BasicStatsSpec extends FlowSpecification {
  sequential
  
  import BasicAggregator._

  val schema = string("name") ~ int("item") ~ int("score")
  val grid = DataGrid(schema) rows (
    ("john", 1, 65), ("john", 3, 78), ("jane", 2, 85), ("jake", 2, 94), ("jake", 1, 70),
    ("jane", 1, 46), ("jake", 4, 62), ("john", 3, 95), ("jane", 3, 50), ("jane", 1, 80))

  "BasicStats" should {
    "aggregate without grouping fields" in {
      val stats = BasicStats() aggr ("score", MIN, MAX, AVG)
      grid --> stats

      assertSchema(int("score_min") ~ int("score_max") ~ double("score_avg"), stats, 0)
      assertOutput(stats, 0, (46, 95, 72.5))
    }
    "aggregate with one grouping field" in {
      val stats = BasicStats() groupBy ("name") aggr ("item", COUNT) aggr ("score", SUM)
      grid --> stats

      assertSchema(string("name") ~ long("item_cnt", false) ~ long("score_sum"), stats, 0)
      assertOutput(stats, 0, ("john", 3, 238), ("jane", 4, 261), ("jake", 3, 226))
    }
    "aggregate with two grouping fields" in {
      val stats = BasicStats() groupBy ("name", "item") aggr ("score", AVG)
      grid --> stats

      assertSchema(string("name") ~ int("item") ~ double("score_avg"), stats, 0)
      assertOutput(stats, 0,
        ("john", 1, 65.0), ("john", 3, 86.5), ("jane", 2, 85.0), ("jake", 2, 94.0), 
        ("jake", 1, 70.0), ("jane", 1, 63.0), ("jake", 4, 62.0), ("jane", 3, 50.0))
    }
    "be unserializable" in assertUnserializable(BasicStats())
  }
}