package com.ignition.frame

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class KafkaInputSpec extends FrameFlowSpecification {

  "KafkaInput" should {
    "construct with defaults" in {
      val step = KafkaInput("zk", "topic", "group")

      step.zkUrl === "zk"
      step.topic === "topic"
      step.groupId === "group"
      step.kafkaProperties must beEmpty
      step.maxRows must beSome(100)
      step.maxTimeout must beSome(60000)
      step.field === "payload"
    }
    "build with helpers" in {
      val step = KafkaInput("zk", "topic", "group") properties ("a" -> "b") maxRows (10) noMaxTimeout

      step.zkUrl === "zk"
      step.topic === "topic"
      step.groupId === "group"
      step.kafkaProperties === Map("a" -> "b")
      step.maxRows must beSome(10)
      step.maxTimeout must beNone
      step.field === "payload"
    }
    "fail without max- condition" in {
      (KafkaInput("zk", "topic", "group").noMaxRows.noMaxTimeout) must throwA[IllegalArgumentException]
    }
  }
}