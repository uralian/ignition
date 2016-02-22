package com.ignition.frame

import scala.collection.JavaConversions.mapAsJavaMap
import scala.util.control.NonFatal
import scala.xml.{ Elem, Node }

import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types.StructType
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jvalue2monadic

import com.ignition.types.{ fieldToStructType, string }
import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.{ RichNodeSeq, intToText, longToText, optToOptText }

import kafka.consumer.{ Consumer, ConsumerConfig }
import kafka.serializer.StringDecoder

/**
 * Reads messages from a Kafka topic, converting each of them into a row with a single column.
 *
 * @author Vlad Orzhekhovskiy
 */
case class KafkaInput(zkUrl: String, topic: String, groupId: String,
                      maxRows: Option[Int] = Some(100), maxTimeout: Option[Long] = Some(60000),
                      kafkaProperties: Map[String, String] = Map.empty, field: String = "payload") extends FrameProducer {

  import KafkaInput._

  def properties(prop: (String, String)*): KafkaInput = copy(kafkaProperties = prop.toMap)

  def maxRows(rows: Int): KafkaInput = copy(maxRows = Some(rows))
  def noMaxRows(): KafkaInput = copy(maxRows = None)

  def maxTimeout(ms: Long): KafkaInput = copy(maxTimeout = Some(ms))
  def noMaxTimeout(): KafkaInput = copy(maxTimeout = None)

  def field(name: String): KafkaInput = copy(field = name)

  if (maxRows.isEmpty && maxTimeout.isEmpty)
    throw new IllegalArgumentException("At least one from maxRows or maxTimeout must be set")

  private val config = {
    val props = new java.util.Properties()
    props.put("group.id", groupId)
    props.put("zookeeper.connect", zkUrl)
    props.putAll(kafkaProperties)
    new ConsumerConfig(props)
  }

  private val decoder = new StringDecoder

  val schema: StructType = string(field)

  protected def compute(implicit runtime: SparkRuntime): DataFrame = {
    val connector = Consumer.create(config)
    val stream = connector.createMessageStreams(Map(topic -> 1), decoder, decoder)(topic).head
    val it = stream.iterator

    def getNext = try {
      it.next.message
    } catch {
      case NonFatal(e) => null
    }

    val start = System.currentTimeMillis
    val messages = Iterator.from(0).map((_, System.currentTimeMillis, getNext)).takeWhile {
      case (index, time, msg) =>
        val dataOk = msg != null
        val indexOk = !maxRows.exists(index >= _) && (!runtime.previewMode || index == 0) 
        val timeOk = !maxTimeout.exists(time - start > _)
        dataOk && indexOk && timeOk
    } map (_._3) toSeq

    connector.shutdown

    val rdd = sc.parallelize(messages map (Row(_)))
    val df = ctx.createDataFrame(rdd, schema)
    optLimit(df, runtime.previewMode)
  }

  override protected def buildSchema(index: Int)(implicit runtime: SparkRuntime): StructType = schema

  def toXml: Elem =
    <node maxRows={ maxRows } maxTimeout={ maxTimeout }>
      <field>{ field }</field>
      <zkUrl>{ zkUrl }</zkUrl>
      <topic>{ topic }</topic>
      <groupId>{ groupId }</groupId>
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
    ("tag" -> tag) ~ ("maxRows" -> maxRows) ~ ("maxTimeout" -> maxTimeout) ~
      ("field" -> field) ~ ("zkUrl" -> zkUrl) ~ ("topic" -> topic) ~ ("groupId" -> groupId) ~
      ("kafkaProperties" -> props)
  }
}

/**
 * Kafka Input companion object.
 */
object KafkaInput {
  val tag = "kafka-input"

  def fromXml(xml: Node) = {
    val maxRows = xml \ "@maxRows" getAsInt
    val maxTimeout = xml \ "@maxTimeout" getAsLong
    val field = xml \ "field" asString
    val zkUrl = xml \ "zkUrl" asString
    val topic = xml \ "topic" asString
    val groupId = xml \ "groupId" asString
    val properties = xml \ "kafkaProperties" \ "property" map { node =>
      val name = node \ "@name" asString
      val value = node.child.head asString

      name -> value
    } toMap

    apply(zkUrl, topic, groupId, maxRows, maxTimeout, properties, field)
  }

  def fromJson(json: JValue) = {
    val maxRows = json \ "maxRows" getAsInt
    val maxTimeout = json \ "maxTimeout" getAsLong
    val field = json \ "field" asString
    val zkUrl = json \ "zkUrl" asString
    val topic = json \ "topic" asString
    val groupId = json \ "groupId" asString
    val properties = (json \ "kafkaProperties" asArray) map { item =>
      val name = item \ "name" asString
      val value = item \ "value" asString

      name -> value
    } toMap

    apply(zkUrl, topic, groupId, maxRows, maxTimeout, properties, field)
  }
}