package com.ignition.frame

import scala.xml.{ Elem, Node }

import org.apache.spark.sql.{ Column, DataFrame }
import org.apache.spark.sql.types.StructType
import org.json4s.JValue
import org.json4s.JsonDSL.{ pair2Assoc, string2jvalue }
import org.json4s.jvalue2monadic

import com.ignition.SparkRuntime
import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.RichNodeSeq

/**
 * Filters the data frame based on a combination of boolean conditions against fields.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Filter(condition: String) extends FrameSplitter(2) {
  import Filter._

  protected def compute(arg: DataFrame, index: Int, preview: Boolean)(implicit runtime: SparkRuntime): DataFrame = {
    val df = optLimit(arg, preview)
    val expr = if (index == 0) condition else s"not($condition)"
    df.filter(expr)
  }

  override protected def buildSchema(index: Int)(implicit runtime: SparkRuntime): StructType =
    input(true).schema

  def toXml: Elem = <node><condition>{ condition }</condition></node>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag) ~ ("condition" -> condition)
}

/**
 * Filter companion object.
 */
object Filter {
  val tag = "filter"

  def apply(column: Column): Filter = apply(column.toString)

  def fromXml(xml: Node) = apply(xml \ "condition" asString)

  def fromJson(json: JValue) = apply(json \ "condition" asString)
}