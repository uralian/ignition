package com.ignition.scripting

import scala.xml.{ Elem, Node }

import com.fasterxml.jackson.databind.ObjectMapper
import com.ignition.data.{ DataRow, DataType, RowMetaData }

import io.gatling.jsonpath.JsonPath

/**
 * JSON path processor, based on https://github.com/gatling/jsonpath implementation.
 *
 * @author Vlad Orzhekhovskiy
 */
case class JsonRowExProcessor(val srcField: String, val query: String) extends RowExProcessor[String] {

  val targetType = implicitly[DataType[String]]

  @transient private lazy val compiled = JsonPath.compile(query)

  private val mapper = new ObjectMapper

  def evaluate(meta: Option[RowMetaData])(row: DataRow): String = compiled.right.map { path =>
    val json = mapper.readValue(row.getString(srcField), classOf[Object])
    path.query(json) mkString
  } match {
    case Left(err) => throw new RuntimeException(err.reason)
    case Right(str) => str
  }

  def toXml: Elem = <json src={ srcField }>{ query }</json>
}

/**
 * JSON processor companion object.
 */
object JsonRowExProcessor {
  def fromXml(xml: Node) = {
    val srcField = (xml \ "@src").text
    val query = xml.text
    new JsonRowExProcessor(srcField, query)
  }
}