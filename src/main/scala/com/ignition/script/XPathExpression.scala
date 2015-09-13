package com.ignition.script

import scala.xml.{ Elem, Node, XML }

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ StringType, StructType }
import org.json4s.JValue
import org.json4s.JsonDSL.{ jobject2assoc, pair2Assoc, pair2jvalue, string2jvalue }
import org.json4s.jvalue2monadic

import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.RichNodeSeq

/**
 * XPath expression row processor.
 *
 * @author Vlad Orzhekhovskiy
 */
case class XPathExpression(val srcField: String, val query: String) extends RowExpression[StringType] {

  val targetType = Some(StringType)

  def evaluate(schema: StructType)(row: Row) = {
    val index = schema.fieldNames.indexOf(srcField)
    val xml = XML.loadString(row.getString(index))
    (xml \\ query) toString
  }

  def toXml: Elem = <xpath source={ srcField }>{ query }</xpath>

  def toJson: JValue = ("type" -> "xpath") ~ ("source" -> srcField) ~ ("query" -> query)
}

/**
 * XPath Expression companion object.
 */
object XPathExpression {

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