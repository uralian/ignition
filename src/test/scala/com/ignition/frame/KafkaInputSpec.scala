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
    "save to/load from xml" in {
      val k1 = KafkaInput("zk", "topic", "group") properties ("a" -> "b") maxRows (10) noMaxTimeout ()
      k1.toXml must ==/(
        <kafka-input maxRows="10">
          <zkUrl>zk</zkUrl>
          <topic>topic</topic>
          <groupId>group</groupId>
          <field>payload</field>
          <kafkaProperties>
            <property name="a">b</property>
          </kafkaProperties>
        </kafka-input>)
      KafkaInput.fromXml(k1.toXml) === k1

      val k2 = KafkaInput("zk", "topic", "group") maxTimeout (200) field ("data")
      k2.toXml must ==/(
        <kafka-input maxRows="100" maxTimeout="200">
          <zkUrl>zk</zkUrl>
          <topic>topic</topic>
          <groupId>group</groupId>
          <field>data</field>
        </kafka-input>)
      KafkaInput.fromXml(k2.toXml) === k2
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._

      val k1 = KafkaInput("zk", "topic", "group") properties ("a" -> "b") maxRows (10) noMaxTimeout ()
      k1.toJson === ("tag" -> "kafka-input") ~ ("topic" -> "topic") ~ ("zkUrl" -> "zk") ~
        ("groupId" -> "group") ~ ("field" -> "payload") ~ ("maxRows" -> 10) ~ ("maxTimeout" -> jNone) ~
        ("kafkaProperties" -> List(("name" -> "a") ~ ("value" -> "b")))
      KafkaInput.fromJson(k1.toJson) === k1

      val k2 = KafkaInput("zk", "topic", "group") maxTimeout (200) field ("data")
      k2.toJson === ("tag" -> "kafka-input") ~ ("topic" -> "topic") ~ ("zkUrl" -> "zk") ~
        ("groupId" -> "group") ~ ("field" -> "data") ~ ("maxRows" -> 100) ~ ("maxTimeout" -> 200) ~
        ("kafkaProperties" -> jNone)
      KafkaInput.fromJson(k2.toJson) === k2
    }
    "be unserializable" in assertUnserializable(KafkaInput("zk", "topic", "group"))
  }
}