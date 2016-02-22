package com.ignition.frame

import scala.collection.JavaConversions.mapAsJavaMap
import scala.xml.{ Elem, Node }

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jvalue2monadic

import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.RichNodeSeq

import kafka.producer.{ KeyedMessage, Producer, ProducerConfig }

/**
 * Posts rows as messages onto a Kafka topic.
 *
 * @author Vlad Orzhekhovskiy
 */
case class KafkaOutput(field: String, topic: String, brokers: Iterable[String] = Nil,
                       kafkaProperties: Map[String, String] = Map.empty) extends FrameTransformer {

  import KafkaOutput._

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

  protected def compute(arg: DataFrame)(implicit runtime: SparkRuntime): DataFrame = {
    val producer = new Producer[String, String](config)

    val df = optLimit(arg, runtime.previewMode)
    df.select(field).collect.foreach { row =>
      val data = row.getString(0)
      val msg = new KeyedMessage[String, String](topic, data)
      producer.send(msg)
    }

    df
  }

  override protected def buildSchema(index: Int)(implicit runtime: SparkRuntime): StructType = input.schema

  def toXml: Elem =
    <node>
      <field>{ field }</field>
      <topic>{ topic }</topic>
      <brokers>{ brokers.mkString(",") }</brokers>
      {
        if (!kafkaProperties.isEmpty)
          <kafkaProperties>
            {
              kafkaProperties map {
                case (name, value) => <property name={ name }>{ value }</property>
              }
            }
          </kafkaProperties>
      }
    </node>.copy(label = tag)

  def toJson: JValue = {
    val props = if (kafkaProperties.isEmpty) None else Some(kafkaProperties.map {
      case (name, value) => ("name" -> name) ~ ("value" -> value)
    })
    ("tag" -> tag) ~ ("field" -> field) ~ ("topic" -> topic) ~ ("brokers" -> brokers.mkString(",")) ~
      ("kafkaProperties" -> props)
  }
}

/**
 * Kafka Output companion object.
 */
object KafkaOutput {
  val tag = "kafka-output"

  def fromXml(xml: Node) = {
    val field = xml \ "field" asString
    val topic = xml \ "topic" asString
    val brokers = (xml \ "brokers" asString) split (",\\s*")
    val properties = xml \ "kafkaProperties" \ "property" map { node =>
      val name = node \ "@name" asString
      val value = node.child.head asString

      name -> value
    } toMap

    apply(field, topic, brokers, properties)
  }

  def fromJson(json: JValue) = {
    val field = json \ "field" asString
    val topic = json \ "topic" asString
    val brokers = (json \ "groupId" asString) split (",\\s*")
    val properties = (json \ "kafkaProperties" asArray) map { item =>
      val name = item \ "name" asString
      val value = item \ "value" asString

      name -> value
    } toMap

    apply(field, topic, brokers, properties)
  }
}