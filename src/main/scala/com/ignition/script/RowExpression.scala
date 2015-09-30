package com.ignition.script

import scala.xml.{ Elem, Node }

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ DataType, StructType }
import org.json4s.{ JString, JValue, jvalue2monadic }

import com.ignition.types.TypeUtils

/**
 * Row expression processor. It evaluates a data row and returns the result.
 *
 * @author Vlad Orzhekhovskiy
 */
trait RowExpression[T <: DataType] extends Serializable {
  /**
   * The static return type of the expression. If None, this means that it cannot be
   * determined statically.
   */
  def targetType: Option[T]

  /**
   * Computes the dynamic return type of the expression.
   */
  def computeTargetType(schema: StructType) = evaluate(schema) _ andThen TypeUtils.typeForValue

  /**
   * Evaluates the data row and computes the result.
   */
  def evaluate(schema: StructType)(row: Row): Any

  /**
   * Converts the expression into XML.
   */
  def toXml: Elem

  /**
   * Converts the expression into JSON.
   */
  def toJson: JValue
}

/**
 * Row Expression companion object.
 */
object RowExpression {

  def fromXml(xml: Node) = xml match {
    case <xpath>{ _* }</xpath> => XPathExpression.fromXml(xml)
    case <json>{ _* }</json> => JsonPathExpression.fromXml(xml)
    case <mvel>{ _* }</mvel> => MvelExpression.fromXml(xml)
  }

  def fromJson(json: JValue) = json \ "type" match {
    case JString("xpath") => XPathExpression.fromJson(json)
    case JString("mvel") => MvelExpression.fromJson(json)
    case JString("json") => JsonPathExpression.fromJson(json)
    case x @ _ => throw new IllegalArgumentException(s"Unknown expression type: $x")
  }
}