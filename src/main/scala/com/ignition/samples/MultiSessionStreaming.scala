package com.ignition.samples

import org.apache.spark.streaming.Seconds
import com.ignition.{ ExecutionException, frame, stream }
import com.ignition.types.{ RichStructType, fieldToRichStruct, int, string }
import com.ignition.SparkHelper

object MultiSessionStreaming extends App {

  def testFrames() = {
    implicit val rt = new frame.DefaultSparkRuntime(SparkHelper.sqlContext)

    // building simple flow grid-->pass
    val grid1 = frame.DataGrid(string("id") ~ string("name") ~ int("weight")) rows (
      (newid, "john", 155),
      (newid, "jane", 190),
      (newid, "jake", 160),
      (newid, "josh", 120))
    val pass = frame.Pass()
    pass.addStepListener(new frame.FrameStepListener {
      override def onAfterStepComputed(event: frame.AfterFrameStepComputed) = event.value.show
    })
    grid1 --> pass
    pass.output

    // adding sql
    val sql = frame.SQLQuery("select SUM(weight) as total from input0")
    grid1 --> sql --> pass
    pass.output

    // replacing grid, removing sql
    val grid2 = frame.DataGrid(string("name") ~ int("weight")) rows (
      ("john", 200),
      ("josh", 100))
    grid2 --> pass
    pass.output

    // restoring sql
    grid2 --> sql --> pass
    pass.output
  }

  def testStreams() = {
    implicit val rt = new stream.DefaultSparkStreamingRuntime(SparkHelper.sqlContext, Seconds(5))

    // building simple flow queue -->pass
    val schema = string("name") ~ int("age")
    val queue1 = stream.QueueInput(schema)
      .addRows(("john", 10), ("jane", 15), ("jake", 18))
      .addRows(("john", 20), ("jane", 25), ("jake", 28))
    val pass = stream.foreach { frame.Pass() }
    pass.addStepListener(new stream.StreamStepListener {
      override def onAfterStepComputed(event: stream.AfterStreamStepComputed) =
        println("output computed: " + event.value)
    })
    pass.addStreamDataListener(new stream.StreamStepDataListener {
      override def onBatchProcessed(event: stream.StreamStepBatchProcessed) =
        event.rows foreach println
    })
    pass.register

    queue1 --> pass

    rt.start
    Thread.sleep(5000)

    rt.restart
    Thread.sleep(5000)

    // adding sql
    val sql = stream.foreach { frame.SQLQuery("select AVG(age) as avage from input0") }
    queue1 --> sql --> pass

    rt.restart
    Thread.sleep(5000)

    // replacing queue, removing sql
    val queue2 = stream.QueueInput(schema)
      .addRows(("jess", 100), ("jill", 200))
      .addRows(("john", 200))
    queue2 --> pass

    rt.restart
    Thread.sleep(5000)

    rt.stop
  }

  testStreams
}