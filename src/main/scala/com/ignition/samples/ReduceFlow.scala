package com.ignition.samples

import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory

import com.ignition.data.{ columnInfo2metaData, int, string }
import com.ignition.workflow.rdd.grid.input.DataGridInput
import com.ignition.workflow.rdd.grid.output.DebugOutput
import com.ignition.workflow.rdd.grid.pair.Reduce
import com.ignition.workflow.rdd.grid.pair.ReduceOp.{ MAX, SUM }

object ReduceFlow extends App {

  val log = LoggerFactory.getLogger(getClass)

  implicit val sc = new SparkContext("local[4]", "test")

  val meta = string("id") ~ int("hour") ~ string("task") ~ int("points")

  val john = "john"
  val jack = "jack"
  val jane = "jane"

  val coding = "coding"
  val design = "design"
  val support = "support"

  val grid = DataGridInput(meta)
    .addRow(john, 5, coding, 3)
    .addRow(john, 3, support, 2)
    .addRow(john, 3, coding, 1)
    .addRow(john, 5, support, 2)
    .addRow(john, 1, design, 2)

    .addRow(jack, 1, design, 3)
    .addRow(jack, 1, coding, 2)
    .addRow(jack, 3, design, 1)
    .addRow(jack, 3, design, 4)

    .addRow(jane, 2, support, 2)
    .addRow(jane, 3, support, 3)
    .addRow(jane, 1, support, 1)
    .addRow(jane, 2, coding, 1)
    .addRow(jane, 2, coding, 4)

  val sumPtsByIdHour = Reduce(Set("id", "hour"), Map("points" -> SUM))
  val maxPtsByIdTask = Reduce(Set("id", "task"), Map("points" -> MAX))
  val allByIdHour = Reduce(Set("id", "hour"), Map("task" -> SUM, "points" -> SUM))
  val ptsByTask = Reduce(Set("task"), Map("points" -> SUM))

  println("SUM(points) by (id, hour):")
  grid connectTo sumPtsByIdHour connectTo DebugOutput() output

  println("MAX(points) by (id, task):")
  grid connectTo maxPtsByIdTask connectTo DebugOutput() output

  println("SUM(task), SUM(points) by (id, hour):")
  grid connectTo allByIdHour connectTo DebugOutput() output

  println("SUM(points) by (task):")
  grid connectTo ptsByTask connectTo DebugOutput() output

  sc.stop
}