package com.ignition.script

import scala.xml.{ Elem, Node }

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ StringType, StructType }
import org.json4s.JValue
import org.json4s.JsonDSL.{ jobject2assoc, pair2Assoc, pair2jvalue, string2jvalue }
import org.json4s.jvalue2monadic

import com.fasterxml.jackson.databind.ObjectMapper
import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.RichNodeSeq

import io.gatling.jsonpath.JsonPath

/**
 * JSON path processor, based on https://github.com/gatling/jsonpath implementation.
 *
 * @author Vlad Orzhekhovskiy
 */
case class JsonPathExpression(val srcField: String, val query: String) extends RowExpression[StringType] {

  @transient private lazy val compiled = JsonPath.compile(query)

  private val mapper = new ObjectMapper

  val targetType = Some(StringType)

  def evaluate(schema: StructType)(row: Row) = compiled.right.map { path =>
    val index = schema.fieldNames.indexOf(srcField)
    val json = mapper.readValue(row.getString(index), classOf[Object])
    path.query(json) mkString
  } match {
    case Left(err) => throw new RuntimeException(err.reason)
    case Right(str) => str
  }

  def toXml: Elem = <json source={ srcField }>{ query }</json>

  def toJson: JValue = ("type" -> "json") ~ ("source" -> srcField) ~ ("query" -> query)
}

/**
 * JsonPath Expression companion object.
 */
object JsonPathExpression {

  def fromXml(xml: Node) = {
    val srcField = xml \ "@source" asString
    val query = xml.child.head asString

    apply(srcField, query)
  }

  def fromJson(json: JValue) = {
    val srcField = json \ "source" asString
    val query = json \ "query" asString

    apply(srcField, query)
  }
}