package com.ignition.samples

import com.ignition.SparkPlug
import com.ignition.flow._
import com.ignition.types._
import com.ignition.flow._

object ReduceFlow extends App {
  import com.ignition.flow.ReduceOp._

  val flow = DataFlow {
    val schema = string("id") ~ int("hour") ~ string("task") ~ int("points")

    val john = "john"
    val jack = "jack"
    val jane = "jane"

    val coding = "coding"
    val design = "design"
    val support = "support"

    val grid = DataGrid(schema) rows (
      (john, 5, coding, 3), (john, 3, support, 2), (john, 3, coding, 1),
      (john, 5, support, 2), (john, 1, design, 2), (jack, 1, design, 3),
      (jack, 1, coding, 2), (jack, 3, design, 1), (jack, 3, design, 4),
      (jane, 2, support, 2), (jane, 3, support, 3), (jane, 1, support, 1),
      (jane, 2, coding, 1), (jane, 2, coding, 4))

    val sumPtsByIdHour = Reduce("points" -> SUM) groupBy ("id", "hour")
    val maxPtsByIdTask = Reduce("points" -> MAX) groupBy ("id", "task")
    val allByIdHour = Reduce("task" -> CONCAT, "points" -> SUM) groupBy ("id", "hour")
    val ptsByTask = Reduce("points" -> SUM) groupBy ("task")

    val (debug1, debug2, debug3, debug4) = (DebugOutput(), DebugOutput(), DebugOutput(), DebugOutput())

    grid --> sumPtsByIdHour --> debug1
    grid --> maxPtsByIdTask --> debug2
    grid --> allByIdHour --> debug3
    grid --> ptsByTask --> debug4

    (debug1, debug2, debug3, debug4)
  }

  SparkPlug.runDataFlow(flow)
}