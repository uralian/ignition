package com.ignition.stream

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.kafka.KafkaUtils

import com.ignition.SparkRuntime
import com.ignition.types.{ fieldToStructType, string }

import kafka.serializer.StringDecoder

/**
 * Creates a text stream from Apache Kafka.
 *
 * @author Vlad Orzhekhovskiy
 */
case class KafkaInput(brokers: Iterable[String], topics: Iterable[String],
                      kafkaProperties: Map[String, String] = Map.empty, field: String = "payload") extends StreamProducer {

  def brokers(hosts: String*): KafkaInput = copy(brokers = hosts)
  def brokers(hosts: String): KafkaInput = copy(brokers = hosts.split(",\\s*"))

  def topics(t: String*): KafkaInput = copy(topics = t)
  def topics(t: String): KafkaInput = copy(topics = t.split(",\\s*"))

  def properties(prop: (String, String)*): KafkaInput = copy(kafkaProperties = prop.toMap)

  val schema = string(field)

  private val kafkaParams = Map("metadata.broker.list" -> brokers.mkString(",")) ++ kafkaProperties

  protected def compute(preview: Boolean)(implicit runtime: SparkRuntime): DataStream = {
    val raw = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics.toSet)

    raw map {
      case (_, value) => new GenericRowWithSchema(Array(value), schema).asInstanceOf[Row]
    }
  }

  protected def computeSchema(implicit runtime: SparkRuntime): StructType = schema

  def toXml: scala.xml.Elem = ???
  def toJson: org.json4s.JValue = ???
}

/**
 * Kafka Input companion object.
 */
object KafkaInput {
  def apply(): KafkaInput = apply(Nil, Nil)
}