package com.ignition.samples

import com.ignition._
import com.ignition.script._

/**
 * @author Vlad Orzhekhovskiy
 */
object KafkaInputOutput extends App {

  if (args.length < 2) {
    Console.err.println(s"""
        |Usage: KafkaInputOutput <brokers> <topics>
        |  <zkUrl> zookeeper connect url
        |  <brokers> comma-separated kafka brokers
        |  <inTopic> topic to consumer from
        |  <groupId> consumer group id
        |  <outTopic> topic to write to
        """.stripMargin)
    sys.exit(1)
  }

  val Array(zkUrl, brokers, inTopic, groupId, outTopic) = args

  val flow = frame.DataFlow {
    val kafkaIn = frame.KafkaInput(zkUrl, inTopic, groupId, Some(5)) properties ("consumer.timeout.ms" -> "5000")
    val select = frame.SelectValues() rename ("payload" -> "data")
    val kafkaOut = frame.KafkaOutput("data", outTopic) brokers (brokers)
    val debug = frame.DebugOutput()

    kafkaIn --> select --> kafkaOut --> debug
  }

  SparkPlug.runDataFlow(flow)
}