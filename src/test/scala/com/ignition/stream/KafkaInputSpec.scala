package com.ignition.stream

import org.json4s.JsonDSL
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class KafkaInputSpec extends StreamFlowSpecification {

  "KafkaInput" should {
    "save to/load from xml" in {
      val ki = KafkaInput(List("broker1", "broker2"), List("topic1"), Map("key" -> "value"), "data")
      ki.toXml must ==/(
        <stream-kafka-input>
          <brokers><broker>broker1</broker><broker>broker2</broker></brokers>
          <topics><topic>topic1</topic></topics>
          <field>data</field>
          <kafkaProperties>
            <property name="key">value</property>
          </kafkaProperties>
        </stream-kafka-input>)
      KafkaInput.fromXml(ki.toXml) === ki
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._

      val ki = KafkaInput(List("broker1", "broker2"), List("topic1"), Map("key" -> "value"), "data")
      ki.toJson === ("tag" -> "stream-kafka-input") ~
        ("brokers" -> List("broker1", "broker2")) ~
        ("topics" -> List("topic1")) ~
        ("field" -> "data") ~
        ("kafkaProperties" -> List(("name" -> "key") ~ ("value" -> "value")))
      KafkaInput.fromJson(ki.toJson) === ki
    }
  }
}