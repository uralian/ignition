package com.ignition.stream

import scala.xml.{ Elem, Node }

import org.apache.spark.sql.{ Column, DataFrame }
import org.json4s.JValue
import org.json4s.JsonDSL.{ pair2Assoc, string2jvalue }
import org.json4s.jvalue2monadic

import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.RichNodeSeq

/**
 * Filters the stream based on a combination of boolean conditions against fields.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Filter(condition: String) extends StreamSplitter(2) {
  import Filter._

  protected def compute(arg: DataStream, index: Int)(implicit runtime: SparkStreamingRuntime): DataStream = {

    val expr = if (index == 0) condition else s"not($condition)"
    val filterFunc = (df: DataFrame) => df.filter(expr)

    transformAsDF(filterFunc)(arg)(runtime.ctx)
  }

  def toXml: Elem = <node><condition>{ condition }</condition></node>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag) ~ ("condition" -> condition)
}

/**
 * Filter companion object.
 */
object Filter {
  val tag = "stream-filter"

  def apply(column: Column): Filter = apply(column.toString)

  def fromXml(xml: Node) = apply(xml \ "condition" asString)

  def fromJson(json: JValue) = apply(json \ "condition" asString)
}