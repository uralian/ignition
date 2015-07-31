package com.ignition.samples

import com.ignition._
import com.ignition.stream._
import com.ignition.types._
import com.ignition.SparkPlug
import com.ignition.frame.DebugOutput
import com.ignition.frame.JoinType
import com.ignition.frame.SQLQuery
import com.ignition.frame.SubFlow
import com.ignition.frame.DataGrid

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
      SubFlow(1, 1) { (input, output) =>
        val grid = DataGrid(string("name") ~ int("age")) rows (("jane", 19), ("john", 35), ("jake", 54))
        val query = SQLQuery("select input0.name, age, score from input0 join input1 on input0.name=input1.name")
        (input.out(0), grid) --> query --> output
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