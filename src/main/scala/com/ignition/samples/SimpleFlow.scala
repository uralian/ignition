package com.ignition.samples

import com.ignition.{ CSource2, frame }
import com.ignition.frame._
import com.ignition.types._
import com.ignition.value2tuple

object SimpleFlow extends App {
  import frame.BasicAggregator._

  val listener = new FrameStepListener with FrameFlowListener {
    override def onBeforeStepComputed(event: BeforeFrameStepComputed) = println(event)
    override def onAfterStepComputed(event: AfterFrameStepComputed) = println(event)
    override def onFrameFlowStarted(event: FrameFlowStarted) = println(event)
    override def onFrameFlowComplete(event: FrameFlowComplete) = println(event)
  }

  val flow = FrameFlow {
    val grid1 = DataGrid(string("id") ~ string("name") ~ int("weight") ~ date("dob")) rows (
      (newid, "john", 155, javaDate(1980, 5, 2)),
      (newid, "jane", 190, javaDate(1982, 4, 25)),
      (newid, "jake", 160, javaDate(1974, 11, 3)),
      (newid, "josh", 120, javaDate(1995, 1, 10)))
    grid1 addStepListener listener

    val grid2 = DataGrid(string("name")) rows ("jane", "josh")

    // first pipeline

    val queryA = SQLQuery("""
      SELECT SUM(weight) AS total, AVG(weight) AS mean, MIN(weight) AS low
      FROM input0 JOIN input1 ON input0.name = input1.name
      WHERE input0.name LIKE 'j%'""")
    queryA addStepListener listener

    val selectA = SelectValues() rename ("mean" -> "average") retype ("average" -> "int")

    val debugA = DebugOutput()

    (grid1, grid2) --> queryA --> selectA --> debugA

    // second pipeline

    val queryB = SQLQuery("SELECT SUBSTR(name, 1, 2) AS name, weight FROM input0")

    val statsB = BasicStats() groupBy "name" add ("weight", AVG, MAX, COUNT_DISTINCT)
    statsB addStepListener listener

    val debugB = DebugOutput()
    
    grid1 --> queryB --> statsB --> debugB

    (debugA, debugB)
  }
  
  flow addFrameFlowListener listener

  frame.Main.runFrameFlow(flow)
}