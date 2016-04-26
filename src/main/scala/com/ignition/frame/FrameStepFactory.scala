package com.ignition.frame

import scala.collection.JavaConverters.asScalaSetConverter
import scala.xml.Node

import org.json4s.{ JValue, jvalue2monadic }

import com.ignition.util.ConfigUtils
import com.ignition.util.JsonUtils.RichJValue
import com.typesafe.config.Config

/**
 * Creates FrameStep instances from Xml and Json.
 *
 * @author Vlad Orzhekhovskiy
 */
object FrameStepFactory extends XmlFrameStepFactory with JsonFrameStepFactory {

  private val xmlParsers = collection.mutable.HashMap.empty[String, Node => FrameStep]
  def registerXml(tag: String, builder: Node => FrameStep) = { xmlParsers(tag) = builder }
  def unregisterXml(tag: String) = { xmlParsers.remove(tag) }

  private val jsonParsers = collection.mutable.HashMap.empty[String, JValue => FrameStep]
  def registerJson(tag: String, builder: JValue => FrameStep) = { jsonParsers(tag) = builder }
  def unregisterJson(tag: String) = { jsonParsers.remove(tag) }

  autoRegisterFromConfig(ConfigUtils.getConfig("custom-steps").getConfig("frame"))

  def fromXml(xml: Node): FrameStep = xml.label match {
    case AddFields.tag         => AddFields.fromXml(xml)
    case BasicStats.tag        => BasicStats.fromXml(xml)
    case CassandraInput.tag    => CassandraInput.fromXml(xml)
    case CassandraOutput.tag   => CassandraOutput.fromXml(xml)
    case CsvFileInput.tag      => CsvFileInput.fromXml(xml)
    case DataGrid.tag          => DataGrid.fromXml(xml)
    case DebugOutput.tag       => DebugOutput.fromXml(xml)
    case Filter.tag            => Filter.fromXml(xml)
    case Formula.tag           => Formula.fromXml(xml)
    case Intersection.tag      => Intersection.fromXml(xml)
    case Invoke.tag            => Invoke.fromXml(xml)
    case JdbcInput.tag         => JdbcInput.fromXml(xml)
    case JdbcOutput.tag        => JdbcOutput.fromXml(xml)
    case Join.tag              => Join.fromXml(xml)
    case JsonFileInput.tag     => JsonFileInput.fromXml(xml)
    case KafkaInput.tag        => KafkaInput.fromXml(xml)
    case KafkaOutput.tag       => KafkaOutput.fromXml(xml)
    case MongoInput.tag        => MongoInput.fromXml(xml)
    case MongoOutput.tag       => MongoOutput.fromXml(xml)
    case Pass.tag              => Pass.fromXml(xml)
    case RangeInput.tag        => RangeInput.fromXml(xml)
    case Reduce.tag            => Reduce.fromXml(xml)
    case RestClient.tag        => RestClient.fromXml(xml)
    case SelectValues.tag      => SelectValues.fromXml(xml)
    case SetVariables.tag      => SetVariables.fromXml(xml)
    case SQLQuery.tag          => SQLQuery.fromXml(xml)
    case FrameSubFlow.tag      => FrameSubFlow.fromXml(xml)
    case TextFileInput.tag     => TextFileInput.fromXml(xml)
    case TextFileOutput.tag    => TextFileOutput.fromXml(xml)
    case TextFolderInput.tag   => TextFolderInput.fromXml(xml)
    case Union.tag             => Union.fromXml(xml)
    case mllib.ColumnStats.tag => mllib.ColumnStats.fromXml(xml)
    case mllib.Correlation.tag => mllib.Correlation.fromXml(xml)
    case mllib.Regression.tag  => mllib.Regression.fromXml(xml)
    case tag                   => xmlParsers(tag).apply(xml)
  }

  def fromJson(json: JValue): FrameStep = (json \ "tag" asString) match {
    case AddFields.tag         => AddFields.fromJson(json)
    case BasicStats.tag        => BasicStats.fromJson(json)
    case CassandraInput.tag    => CassandraInput.fromJson(json)
    case CassandraOutput.tag   => CassandraOutput.fromJson(json)
    case CsvFileInput.tag      => CsvFileInput.fromJson(json)
    case DataGrid.tag          => DataGrid.fromJson(json)
    case DebugOutput.tag       => DebugOutput.fromJson(json)
    case Filter.tag            => Filter.fromJson(json)
    case Formula.tag           => Formula.fromJson(json)
    case Intersection.tag      => Intersection.fromJson(json)
    case Invoke.tag            => Invoke.fromJson(json)
    case JdbcInput.tag         => JdbcInput.fromJson(json)
    case JdbcOutput.tag        => JdbcOutput.fromJson(json)
    case Join.tag              => Join.fromJson(json)
    case JsonFileInput.tag     => JsonFileInput.fromJson(json)
    case KafkaInput.tag        => KafkaInput.fromJson(json)
    case KafkaOutput.tag       => KafkaOutput.fromJson(json)
    case MongoInput.tag        => MongoInput.fromJson(json)
    case MongoOutput.tag       => MongoOutput.fromJson(json)
    case Pass.tag              => Pass.fromJson(json)
    case RangeInput.tag        => RangeInput.fromJson(json)
    case Reduce.tag            => Reduce.fromJson(json)
    case RestClient.tag        => RestClient.fromJson(json)
    case SelectValues.tag      => SelectValues.fromJson(json)
    case SetVariables.tag      => SetVariables.fromJson(json)
    case SQLQuery.tag          => SQLQuery.fromJson(json)
    case FrameSubFlow.tag      => FrameSubFlow.fromJson(json)
    case TextFileInput.tag     => TextFileInput.fromJson(json)
    case TextFileOutput.tag    => TextFileOutput.fromJson(json)
    case TextFolderInput.tag   => TextFolderInput.fromJson(json)
    case Union.tag             => Union.fromJson(json)
    case mllib.ColumnStats.tag => mllib.ColumnStats.fromJson(json)
    case mllib.Correlation.tag => mllib.Correlation.fromJson(json)
    case mllib.Regression.tag  => mllib.Regression.fromJson(json)
    case tag                   => jsonParsers(tag).apply(json)
  }

  /**
   * Registers XML and JSON factories based on the configuration parameters.
   */
  private def autoRegisterFromConfig(config: Config) = {
    val keys = config.root.keySet.asScala
    keys foreach { tag =>
      val cfg = config.getConfig(tag)

      val xmlFactory = com.ignition.getClassInstance[XmlFrameStepFactory](cfg.getString("xmlFactory"))
      registerXml(tag, xmlFactory.fromXml)

      val jsonFactory = com.ignition.getClassInstance[JsonFrameStepFactory](cfg.getString("jsonFactory"))
      registerJson(tag, jsonFactory.fromJson)
    }
  }
}