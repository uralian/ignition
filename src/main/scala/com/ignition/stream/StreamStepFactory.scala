package com.ignition.stream

import scala.xml.Node

import org.apache.spark.sql.DataFrame
import org.json4s.{ JValue, jvalue2monadic }

import com.ignition.{ JsonStepFactory, SubFlow, XmlStepFactory, Step }
import com.ignition.util.JsonUtils.RichJValue

/**
 * Creates StreamStep instances from Xml and Json.
 *
 * @author Vlad Orzhekhovskiy
 */
object StreamStepFactory
    extends XmlStepFactory[StreamStep, DataStream, SparkStreamingRuntime]
    with JsonStepFactory[StreamStep, DataStream, SparkStreamingRuntime] {

  private val xmlParsers = collection.mutable.HashMap.empty[String, Node => StreamStep]
  def registerXml(tag: String, builder: Node => StreamStep) = { xmlParsers(tag) = builder }

  private val jsonParsers = collection.mutable.HashMap.empty[String, JValue => StreamStep]
  def registerJson(tag: String, builder: JValue => StreamStep) = { jsonParsers(tag) = builder }

  def fromXml(xml: Node): StreamStep = xml.label match {
    case DSAStreamInput.tag  => DSAStreamInput.fromXml(xml)
    case DSAStreamOutput.tag => DSAStreamOutput.fromXml(xml)
    case Filter.tag          => Filter.fromXml(xml)
    case Foreach.tag         => Foreach.fromXml(xml)
    case Join.tag            => Join.fromXml(xml)
    case KafkaInput.tag      => KafkaInput.fromXml(xml)
    case MvelStateUpdate.tag => MvelStateUpdate.fromXml(xml)
    case QueueInput.tag      => QueueInput.fromXml(xml)
    case SetVariables.tag    => SetVariables.fromXml(xml)
    case Window.tag          => Window.fromXml(xml)
    case StreamSubFlow.tag   => StreamSubFlow.fromXml(xml)
    case tag                 => xmlParsers(tag).apply(xml)
  }

  def fromJson(json: JValue): StreamStep = (json \ "tag" asString) match {
    case DSAStreamInput.tag  => DSAStreamInput.fromJson(json)
    case DSAStreamOutput.tag => DSAStreamOutput.fromJson(json)
    case Filter.tag          => Filter.fromJson(json)
    case Foreach.tag         => Foreach.fromJson(json)
    case Join.tag            => Join.fromJson(json)
    case KafkaInput.tag      => KafkaInput.fromJson(json)
    case MvelStateUpdate.tag => MvelStateUpdate.fromJson(json)
    case QueueInput.tag      => QueueInput.fromJson(json)
    case SetVariables.tag    => SetVariables.fromJson(json)
    case Window.tag          => Window.fromJson(json)
    case StreamSubFlow.tag   => StreamSubFlow.fromJson(json)
    case tag                 => jsonParsers(tag).apply(json)
  }
}