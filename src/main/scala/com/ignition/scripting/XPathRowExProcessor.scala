package com.ignition.scripting

import scala.xml.{ Elem, Node, XML }

import com.ignition.data.{ DataRow, DataType, RowMetaData }

/**
 * XPath expression row processor.
 *
 * @author Vlad Orzhekhovskiy
 */
case class XPathRowExProcessor(val srcField: String, val query: String) extends RowExProcessor[String] {

  val targetType = implicitly[DataType[String]]

  def evaluate(meta: Option[RowMetaData])(row: DataRow): String = {
    val xml = XML.loadString(row.getString(srcField))
    (xml \\ query) toString
  }

  def toXml: Elem = <xpath src={ srcField }>{ query }</xpath>
}

/**
 * XPath processor companion object.
 */
object XPathRowExProcessor {
  def fromXml(xml: Node) = {
    val srcField = (xml \ "@src").text
    val query = xml.text
    new XPathRowExProcessor(srcField, query)
  }
}