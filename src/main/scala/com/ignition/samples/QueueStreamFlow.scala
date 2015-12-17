package com.ignition.samples

import scala.concurrent.{ Await, TimeoutException }
import scala.concurrent.duration.Duration

import com.ignition.frame.{ DebugOutput, FrameSubTransformer, Reduce, ReduceOp, SQLQuery, SelectValues }
import com.ignition.stream
import com.ignition.stream.{ Filter, QueueInput, StreamFlow, foreach }
import com.ignition.types.{ RichStructType, fieldToRichStruct, int, string }

object QueueStreamFlow extends App {
  import com.ignition.frame.ReduceOp._

  val flow = StreamFlow {
    val schema = string("name") ~ int("age") ~ int("score")
    val queue = QueueInput(schema).
      addRows(("jane", 25, 94), ("john", 32, 83)).
      addRows(("jake", 29, 77)).
      addRows(("josh", 41, 90), ("jill", 44, 89), ("jess", 34, 65)).
      addRows(("judd", 19, 95))

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

    queue --> foreach(DebugOutput() title "input") --> filter --> (calcTrue, calcFalse)
    (calcTrue, calcFalse)
  }

  val (id, f) = stream.Main.startStreamFlow(flow)
  println(s"Flow #$id started")
  Await.ready(f, Duration.Inf)
}