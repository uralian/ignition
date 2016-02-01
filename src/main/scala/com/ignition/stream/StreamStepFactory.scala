package com.ignition.stream

import scala.collection.JavaConverters.asScalaSetConverter
import scala.xml.Node

import org.json4s.{ JValue, jvalue2monadic }

import com.ignition.util.ConfigUtils
import com.ignition.util.JsonUtils.RichJValue
import com.typesafe.config.Config

/**
 * Creates StreamStep instances from Xml and Json.
 *
 * @author Vlad Orzhekhovskiy
 */
object StreamStepFactory extends XmlStreamStepFactory with JsonStreamStepFactory {

  private val xmlParsers = collection.mutable.HashMap.empty[String, Node => StreamStep]
  def registerXml(tag: String, builder: Node => StreamStep) = { xmlParsers(tag) = builder }
  def unregisterXml(tag: String) = { xmlParsers.remove(tag) }

  private val jsonParsers = collection.mutable.HashMap.empty[String, JValue => StreamStep]
  def registerJson(tag: String, builder: JValue => StreamStep) = { jsonParsers(tag) = builder }
  def unregisterJson(tag: String) = { jsonParsers.remove(tag) }

  autoRegisterFromConfig(ConfigUtils.getConfig("custom-steps").getConfig("stream"))

  def fromXml(xml: Node): StreamStep = xml.label match {
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

  /**
   * Registers XML and JSON factories based on the configuration parameters.
   */
  private def autoRegisterFromConfig(config: Config) = {
    val keys = config.root.keySet.asScala
    keys foreach { tag =>
      val cfg = config.getConfig(tag)

      val xmlFactory = com.ignition.getClassInstance[XmlStreamStepFactory](cfg.getString("xmlFactory"))
      registerXml(tag, xmlFactory.fromXml)

      val jsonFactory = com.ignition.getClassInstance[JsonStreamStepFactory](cfg.getString("jsonFactory"))
      registerJson(tag, jsonFactory.fromJson)
    }
  }
}