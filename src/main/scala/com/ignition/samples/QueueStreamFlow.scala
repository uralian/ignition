package com.ignition.samples

import scala.concurrent.Await
import scala.concurrent.duration.Duration

import com.ignition.frame.{ DebugOutput, FrameSubTransformer, Reduce, ReduceOp, SQLQuery, SelectValues }
import com.ignition.stream
import com.ignition.stream._
import com.ignition.types.{ RichStructType, fieldToRichStruct, int, string }

object QueueStreamFlow extends App {
  import com.ignition.frame.ReduceOp._

  val listener = new StreamStepListener with StreamStepDataListener with StreamFlowListener {
    override def onBeforeStepComputed(event: BeforeStreamStepComputed) = println(event)
    override def onAfterStepComputed(event: AfterStreamStepComputed) = println(event)
    override def onBatchProcessed(event: StreamStepBatchProcessed) = println(event)
    override def onStreamFlowStarted(event: StreamFlowStarted) = println(event)
    override def onStreamFlowTerminated(event: StreamFlowTerminated) = println(event)
  }

  val flow = StreamFlow {
    val schema = string("name") ~ int("age") ~ int("score")
    val queue = QueueInput(schema).
      addRows(("jane", 25, 94), ("john", 32, 83)).
      addRows(("jake", 29, 77)).
      addRows(("josh", 41, 90), ("jill", 44, 89), ("jess", 34, 65)).
      addRows(("judd", 19, 95))
    queue addStepListener listener
    queue addStreamDataListener listener

    val calcTrue = foreach {
      FrameSubTransformer {
        val sql = SQLQuery("select MIN(age) AS min_age, AVG(score) AS avg_score FROM input0")
        val select = SelectValues() retype ("avg_score" -> "int")
        val debug = DebugOutput() title "true"
        sql --> select --> debug
        (sql.in(0), debug)
      }
    }

    val calcFalse = foreach {
      FrameSubTransformer {
        val reduce = Reduce("age" -> MAX, "score" -> SUM)
        val debug = DebugOutput() title "false"
        reduce --> debug
        (reduce, debug)
      }
    }

    val filter = Filter("age < 30")
    filter addStepListener listener
    filter addStreamDataListener listener

    queue --> foreach(DebugOutput() title "input") --> filter --> (calcTrue, calcFalse)
    (calcTrue, calcFalse)
  }

  flow addStreamFlowListener listener

  stream.Main.startAndWait(flow)
}