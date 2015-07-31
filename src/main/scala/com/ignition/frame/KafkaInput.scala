package com.ignition.frame

import scala.collection.JavaConversions.mapAsJavaMap
import scala.util.control.NonFatal

import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types.StructType

import com.ignition.SparkRuntime
import com.ignition.types.{ fieldToStructType, string }

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

  def properties(prop: (String, String)*): KafkaInput = copy(kafkaProperties = prop.toMap)

  def maxRows(rows: Int): KafkaInput = copy(maxRows = Some(rows))
  def noMaxRows: KafkaInput = copy(maxRows = None)

  def maxTimeout(ms: Long): KafkaInput = copy(maxTimeout = Some(ms))
  def noMaxTimeout: KafkaInput = copy(maxTimeout = None)

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

  protected def compute(limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
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
        val indexOk = !maxRows.exists(index >= _) && !limit.exists(index >= _)
        val timeOk = !maxTimeout.exists(time - start > _)
        dataOk && indexOk && timeOk
    } map (_._3) toSeq

    connector.shutdown

    val rdd = sc.parallelize(messages map (Row(_)))
    val df = ctx.createDataFrame(rdd, schema)
    optLimit(df, limit)
  }

  protected def computeSchema(implicit runtime: SparkRuntime): StructType = schema
}