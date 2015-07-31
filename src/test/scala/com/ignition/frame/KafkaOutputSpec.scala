package com.ignition.frame

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class KafkaOutputSpec extends FrameFlowSpecification {
  "KafkaOutput" should {
    "construct with defaults" in {
      val step = KafkaOutput("field", "topic")

      step.field === "field"
      step.topic === "topic"
      step.brokers must beEmpty
      step.kafkaProperties must beEmpty
    }
    "build with helpers" in {
      val step = KafkaOutput("field", "topic") brokers("b1, b2,b3") properties("a" -> "b")
      step.brokers.toList === List("b1", "b2", "b3")
      step.kafkaProperties === Map("a" -> "b")
    }
  }
}