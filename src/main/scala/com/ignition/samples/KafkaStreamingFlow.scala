package com.ignition.samples

import com.ignition.{ CSource2, SparkPlug }
import com.ignition.frame.{ DataGrid, DebugOutput, FrameSubTransformer, JoinType, SQLQuery }
import com.ignition.stream.{ Join, KafkaInput, QueueInput, StreamFlow, foreach }
import com.ignition.types.{ fieldToRichStruct, int, string }

object KafkaStreamingFlow extends App {

  if (args.length < 2) {
    Console.err.println(s"""
        |Usage: KafkaStreamingFlow <brokers> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |
        """.stripMargin)
    sys.exit(1)
  }

  val Array(brokers, topics) = args

  val flow = StreamFlow {
    val queue = QueueInput(string("name") ~ int("score")).
      addRows(("john", 10), ("jane", 15), ("jake", 18)).
      addRows(("john", 20), ("jane", 25), ("jake", 28)).
      addRows(("john", 30), ("jane", 35), ("jake", 38))

    val sql = foreach {
      FrameSubTransformer {
        val grid = DataGrid(string("name") ~ int("age")) rows (("jane", 19), ("john", 35), ("jake", 54))
        val query = SQLQuery("select input0.name, age, score from input0 join input1 on input0.name=input1.name")
        grid --> query.in(1)
        (query.in(0), query)
      }
    }

    val kafka = KafkaInput() brokers (brokers) topics (topics)

    val join = Join() joinType (JoinType.OUTER)

    val debug = foreach { DebugOutput() }

    queue --> sql
    (kafka, sql.out(0)) --> join --> debug
  }

  SparkPlug.startStreamFlow(flow)
}