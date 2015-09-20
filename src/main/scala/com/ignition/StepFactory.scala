package com.ignition

import scala.xml.Node

import org.json4s.{ JValue, jvalue2monadic }

import com.ignition.util.JsonUtils.RichJValue

/**
 * @author Vlad Orzhekhovskiy
 */
object StepFactory {

  private val xmlParsers = collection.mutable.HashMap.empty[String, Node => Step[_]]
  private val jsonParsers = collection.mutable.HashMap.empty[String, JValue => Step[_]]

  def registerXml(tag: String, builder: Node => Step[_]) = { xmlParsers(tag) = builder }

  def fromXml(xml: Node): Step[_] = xml.label match {
    case frame.BasicStats.tag => frame.BasicStats.fromXml(xml)
    case frame.CassandraInput.tag => frame.CassandraInput.fromXml(xml)
    case frame.CassandraOutput.tag => frame.CassandraOutput.fromXml(xml)
    case frame.CsvFileInput.tag => frame.CsvFileInput.fromXml(xml)
    case frame.DataGrid.tag => frame.DataGrid.fromXml(xml)
    case frame.DebugOutput.tag => frame.DebugOutput.fromXml(xml)
    case frame.Filter.tag => frame.Filter.fromXml(xml)
    case frame.Formula.tag => frame.Formula.fromXml(xml)
    case frame.Intersection.tag => frame.Intersection.fromXml(xml)
    case frame.Join.tag => frame.Join.fromXml(xml)
    case frame.JsonFileInput.tag => frame.JsonFileInput.fromXml(xml)
    case frame.KafkaInput.tag => frame.KafkaInput.fromXml(xml)
    case frame.KafkaOutput.tag => frame.KafkaOutput.fromXml(xml)
    case frame.MongoInput.tag => frame.MongoInput.fromXml(xml)
    case frame.MongoOutput.tag => frame.MongoOutput.fromXml(xml)
    case frame.Reduce.tag => frame.Reduce.fromXml(xml)
    case frame.RestClient.tag => frame.RestClient.fromXml(xml)
    case frame.SelectValues.tag => frame.SelectValues.fromXml(xml)
    case frame.SetVariables.tag => frame.SetVariables.fromXml(xml)
    case frame.SQLQuery.tag => frame.SQLQuery.fromXml(xml)
    case frame.SubFlow.tag => frame.SubFlow.fromXml(xml)
    case frame.TextFileInput.tag => frame.TextFileInput.fromXml(xml)
    case frame.TextFileOutput.tag => frame.TextFileOutput.fromXml(xml)
    case frame.TextFolderInput.tag => frame.TextFolderInput.fromXml(xml)
    case frame.Union.tag => frame.Union.fromXml(xml)
    case tag => xmlParsers(tag).apply(xml)
  }

  def registerJson(tag: String, builder: JValue => Step[_]) = { jsonParsers(tag) = builder }

  def fromJson(json: JValue): Step[_] = (json \ "tag" asString) match {
    case frame.BasicStats.tag => frame.BasicStats.fromJson(json)
    case frame.CassandraInput.tag => frame.CassandraInput.fromJson(json)
    case frame.CassandraOutput.tag => frame.CassandraOutput.fromJson(json)
    case frame.CsvFileInput.tag => frame.CsvFileInput.fromJson(json)
    case frame.DataGrid.tag => frame.DataGrid.fromJson(json)
    case frame.DebugOutput.tag => frame.DebugOutput.fromJson(json)
    case frame.Filter.tag => frame.Filter.fromJson(json)
    case frame.Formula.tag => frame.Formula.fromJson(json)
    case frame.Intersection.tag => frame.Intersection.fromJson(json)
    case frame.Join.tag => frame.Join.fromJson(json)
    case frame.JsonFileInput.tag => frame.JsonFileInput.fromJson(json)
    case frame.KafkaInput.tag => frame.KafkaInput.fromJson(json)
    case frame.KafkaOutput.tag => frame.KafkaOutput.fromJson(json)
    case frame.MongoInput.tag => frame.MongoInput.fromJson(json)
    case frame.MongoOutput.tag => frame.MongoOutput.fromJson(json)
    case frame.Reduce.tag => frame.Reduce.fromJson(json)
    case frame.RestClient.tag => frame.RestClient.fromJson(json)
    case frame.SelectValues.tag => frame.SelectValues.fromJson(json)
    case frame.SetVariables.tag => frame.SetVariables.fromJson(json)
    case frame.SQLQuery.tag => frame.SQLQuery.fromJson(json)
    case frame.SubFlow.tag => frame.SubFlow.fromJson(json)
    case frame.TextFileInput.tag => frame.TextFileInput.fromJson(json)
    case frame.TextFileOutput.tag => frame.TextFileOutput.fromJson(json)
    case frame.TextFolderInput.tag => frame.TextFolderInput.fromJson(json)
    case frame.Union.tag => frame.Union.fromJson(json)
    case tag => jsonParsers(tag).apply(json)
  }
}