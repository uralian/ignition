package com.ignition.frame

import scala.xml.Node

import org.json4s.{ JValue, jvalue2monadic }

import com.ignition.SubFlow
import com.ignition.util.JsonUtils.RichJValue

/**
 * @author Vlad Orzhekhovskiy
 */
object StepFactory {

  private val xmlParsers = collection.mutable.HashMap.empty[String, Node => FrameStep]
  def registerXml(tag: String, builder: Node => FrameStep) = { xmlParsers(tag) = builder }

  private val jsonParsers = collection.mutable.HashMap.empty[String, JValue => FrameStep]
  def registerJson(tag: String, builder: JValue => FrameStep) = { jsonParsers(tag) = builder }

  def fromXml(xml: Node): FrameStep = xml.label match {
    case BasicStats.tag => BasicStats.fromXml(xml)
    case CassandraInput.tag => CassandraInput.fromXml(xml)
    case CassandraOutput.tag => CassandraOutput.fromXml(xml)
    case CsvFileInput.tag => CsvFileInput.fromXml(xml)
    case DataGrid.tag => DataGrid.fromXml(xml)
    case DebugOutput.tag => DebugOutput.fromXml(xml)
    case Filter.tag => Filter.fromXml(xml)
    case Formula.tag => Formula.fromXml(xml)
    case Intersection.tag => Intersection.fromXml(xml)
    case Join.tag => Join.fromXml(xml)
    case JsonFileInput.tag => JsonFileInput.fromXml(xml)
    case KafkaInput.tag => KafkaInput.fromXml(xml)
    case KafkaOutput.tag => KafkaOutput.fromXml(xml)
    case MongoInput.tag => MongoInput.fromXml(xml)
    case MongoOutput.tag => MongoOutput.fromXml(xml)
    case Reduce.tag => Reduce.fromXml(xml)
    case RestClient.tag => RestClient.fromXml(xml)
    case SelectValues.tag => SelectValues.fromXml(xml)
    case SetVariables.tag => SetVariables.fromXml(xml)
    case SQLQuery.tag => SQLQuery.fromXml(xml)
    case SubFlow.tag => FrameSubFlow.fromXml(xml)
    case TextFileInput.tag => TextFileInput.fromXml(xml)
    case TextFileOutput.tag => TextFileOutput.fromXml(xml)
    case TextFolderInput.tag => TextFolderInput.fromXml(xml)
    case Union.tag => Union.fromXml(xml)
    case mllib.ColumnStats.tag => mllib.ColumnStats.fromXml(xml)
    case mllib.Correlation.tag => mllib.Correlation.fromXml(xml)
    case mllib.Regression.tag => mllib.Regression.fromXml(xml)
    case tag => xmlParsers(tag).apply(xml)
  }

  def fromJson(json: JValue): FrameStep = (json \ "tag" asString) match {
    case BasicStats.tag => BasicStats.fromJson(json)
    case CassandraInput.tag => CassandraInput.fromJson(json)
    case CassandraOutput.tag => CassandraOutput.fromJson(json)
    case CsvFileInput.tag => CsvFileInput.fromJson(json)
    case DataGrid.tag => DataGrid.fromJson(json)
    case DebugOutput.tag => DebugOutput.fromJson(json)
    case Filter.tag => Filter.fromJson(json)
    case Formula.tag => Formula.fromJson(json)
    case Intersection.tag => Intersection.fromJson(json)
    case Join.tag => Join.fromJson(json)
    case JsonFileInput.tag => JsonFileInput.fromJson(json)
    case KafkaInput.tag => KafkaInput.fromJson(json)
    case KafkaOutput.tag => KafkaOutput.fromJson(json)
    case MongoInput.tag => MongoInput.fromJson(json)
    case MongoOutput.tag => MongoOutput.fromJson(json)
    case Reduce.tag => Reduce.fromJson(json)
    case RestClient.tag => RestClient.fromJson(json)
    case SelectValues.tag => SelectValues.fromJson(json)
    case SetVariables.tag => SetVariables.fromJson(json)
    case SQLQuery.tag => SQLQuery.fromJson(json)
    case SubFlow.tag => FrameSubFlow.fromJson(json)
    case TextFileInput.tag => TextFileInput.fromJson(json)
    case TextFileOutput.tag => TextFileOutput.fromJson(json)
    case TextFolderInput.tag => TextFolderInput.fromJson(json)
    case Union.tag => Union.fromJson(json)
    case mllib.ColumnStats.tag => mllib.ColumnStats.fromJson(json)
    case mllib.Correlation.tag => mllib.Correlation.fromJson(json)
    case mllib.Regression.tag => mllib.Regression.fromJson(json)
    case tag => jsonParsers(tag).apply(json)
  }
}