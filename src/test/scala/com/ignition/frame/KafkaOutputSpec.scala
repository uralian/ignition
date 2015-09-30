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
      val step = KafkaOutput("field", "topic") brokers ("b1, b2,b3") properties ("a" -> "b")
      step.brokers.toList === List("b1", "b2", "b3")
      step.kafkaProperties === Map("a" -> "b")
    }
    "save to/load from xml" in {
      val k1 = KafkaOutput("field", "topic") brokers ("b1, b2") properties ("a" -> "b")
      k1.toXml must ==/(
        <kafka-output>
          <field>field</field>
          <topic>topic</topic>
          <brokers>b1,b2</brokers>
          <kafkaProperties>
            <property name="a">b</property>
          </kafkaProperties>
        </kafka-output>)
      KafkaOutput.fromXml(k1.toXml) === k1
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._

      val k1 = KafkaOutput("field", "topic") brokers ("b1, b2") properties ("a" -> "b")
      k1.toJson === ("tag" -> "kafka-output") ~ ("field" -> "field") ~ ("topic" -> "topic") ~
        ("brokers" -> "b1,b2") ~ ("kafkaProperties" -> List(("name" -> "a") ~ ("value" -> "b")))
    }
    "be unserializable" in assertUnserializable(KafkaOutput("field", "topic"))
  }
}