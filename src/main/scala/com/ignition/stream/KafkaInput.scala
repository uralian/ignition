package com.ignition.stream

import scala.xml.{ Elem, Node }

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.streaming.kafka.KafkaUtils
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jvalue2monadic

import com.ignition.types.{ fieldToStructType, string }
import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.RichNodeSeq

import kafka.serializer.StringDecoder

/**
 * Creates a text stream from Apache Kafka.
 *
 * @author Vlad Orzhekhovskiy
 */
case class KafkaInput(brokers: Iterable[String], topics: Iterable[String],
                      kafkaProperties: Map[String, String] = Map.empty, field: String = "payload") extends StreamProducer {

  import KafkaInput._

  def brokers(hosts: String*): KafkaInput = copy(brokers = hosts)
  def brokers(hosts: String): KafkaInput = copy(brokers = hosts.split(",\\s*"))

  def topics(t: String*): KafkaInput = copy(topics = t)
  def topics(t: String): KafkaInput = copy(topics = t.split(",\\s*"))

  def properties(prop: (String, String)*): KafkaInput = copy(kafkaProperties = prop.toMap)

  val schema = string(field)

  private val kafkaParams = Map("metadata.broker.list" -> brokers.mkString(",")) ++ kafkaProperties

  protected def compute(implicit runtime: SparkStreamingRuntime): DataStream = {
    val raw = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics.toSet)

    raw map {
      case (_, value) => new GenericRowWithSchema(Array(value), schema).asInstanceOf[Row]
    }
  }

  def toXml: Elem =
    <node>
      <field>{ field }</field>
      <brokers>
        { brokers map (b => <broker>{ b }</broker>) }
      </brokers>
      <topics>
        { topics map (t => <topic>{ t }</topic>) }
      </topics>
      {
        if (!kafkaProperties.isEmpty)
          <kafkaProperties>
            {
              kafkaProperties map {
                case (name, value)=> <property name={ name }>{ value }</property>
              }
            }
          </kafkaProperties>
      }
    </node>.copy(label = tag)

  def toJson: JValue = {
    val props = if (kafkaProperties.isEmpty) None else Some(kafkaProperties.map {
      case (name, value) => ("name" -> name) ~ ("value" -> value)
    })
    ("tag" -> tag) ~ ("field" -> field) ~ ("brokers" -> brokers) ~ ("topics" -> topics) ~ ("kafkaProperties" -> props)
  }
}

/**
 * Kafka Input companion object.
 */
object KafkaInput {
  val tag = "stream-kafka-input"

  def apply(): KafkaInput = apply(Nil, Nil)

  def fromXml(xml: Node) = {
    val field = xml \ "field" asString
    val brokers = xml \\ "broker" map (_.asString)
    val topics = xml \\ "topic" map (_.asString)

    val properties = xml \ "kafkaProperties" \ "property" map { node =>
      val name = node \ "@name" asString
      val value = node.child.head asString

      name -> value
    } toMap

    apply(brokers, topics, properties, field)
  }

  def fromJson(json: JValue) = {
    val field = json \ "field" asString
    val brokers = (json \ "brokers" asArray) map (_.asString)
    val topics = (json \ "topics" asArray) map (_.asString)

    val properties = (json \ "kafkaProperties" asArray) map { item =>
      val name = item \ "name" asString
      val value = item \ "value" asString

      name -> value
    } toMap

    apply(brokers, topics, properties, field)
  }
}