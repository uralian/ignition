package com.ignition.samples

import com.ignition.SparkPlug
import com.ignition.frame.{ DebugOutput, FrameSubTransformer, Reduce, ReduceOp, SQLQuery, SelectValues }
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
        val debug = DebugOutput()
        sql --> select --> debug
        (sql.in(0), debug)
      }
    }

    val calcFalse = foreach {
      FrameSubTransformer {
        val reduce = Reduce("age" -> MAX, "score" -> SUM)
        val debug = DebugOutput()
        reduce --> debug
        (reduce, debug)
      }
    }

    val filter = Filter("age < 30")

    queue --> filter --> (calcTrue, calcFalse)
    (calcTrue, calcFalse)
  }

  SparkPlug.startStreamFlow(flow)
}