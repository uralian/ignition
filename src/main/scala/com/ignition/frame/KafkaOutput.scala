package com.ignition.frame

import scala.collection.JavaConversions.mapAsJavaMap

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import com.ignition.SparkRuntime

import kafka.producer.{ KeyedMessage, Producer, ProducerConfig }

/**
 * Posts rows as messages onto a Kafka topic.
 *
 * @author Vlad Orzhekhovskiy
 */
case class KafkaOutput(field: String, topic: String, brokers: Iterable[String] = Nil,
                       kafkaProperties: Map[String, String] = Map.empty) extends FrameTransformer {

  def brokers(hosts: String*): KafkaOutput = copy(brokers = hosts)
  def brokers(hosts: String): KafkaOutput = copy(brokers = hosts.split(",\\s*"))

  def properties(prop: (String, String)*): KafkaOutput = copy(kafkaProperties = prop.toMap)

  private val config = {
    val props = new java.util.Properties()
    props.put("metadata.broker.list", brokers.mkString(","))
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.putAll(kafkaProperties)
    new ProducerConfig(props)
  }

  protected def compute(arg: DataFrame, limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
    val producer = new Producer[String, String](config)

    val df = optLimit(arg, limit)
    df.select(field).collect.foreach { row =>
      val data = row.getString(0)
      val msg = new KeyedMessage[String, String](topic, data)
      producer.send(msg)
    }

    df
  }

  protected def computeSchema(inSchema: StructType)(implicit runtime: SparkRuntime): StructType = inSchema
}