package com.ignition.scripting

import scala.xml.{ Elem, Node, Utility }

import com.ignition.data.{ DataRow, DataType, RowMetaData }

/**
 * Row expression processor. It evaluates a data row and returns the result.
 *
 * @author Vlad Orzhekhovskiy
 */
trait RowExProcessor[T] extends Serializable {

  /**
   * Processor output type.
   */
  val targetType: DataType[T]

  /**
   * Evaluates the data row and computes the result.
   */
  def evaluate(metaData: Option[RowMetaData])(row: DataRow): T

  /**
   * Serializes the processor into XML.
   */
  def toXml: Elem
}

/**
 * Row processor companion object.
 */
object RowExProcessor {
  def fromXml(xml: Node) = Utility.trim(xml.child.head) match {
    case n @ <mvel>{ _* }</mvel> => MvelRowExProcessor.fromXml(n)
    case n @ <xpath>{ _* }</xpath> => XPathRowExProcessor.fromXml(n)
    case n @ <json>{ _* }</json> => JsonRowExProcessor.fromXml(n)
  }
}