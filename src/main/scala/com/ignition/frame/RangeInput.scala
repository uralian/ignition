package com.ignition.frame

import scala.xml.{ Elem, Node }

import org.apache.spark.sql.DataFrame
import org.json4s.JValue
import org.json4s.JsonDSL.{ jobject2assoc, long2jvalue, pair2Assoc, pair2jvalue, string2jvalue }
import org.json4s.jvalue2monadic

import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.RichNodeSeq

/**
 * Generates a set of numeric values in a given range.
 */
case class RangeInput(start: Long, end: Long, inc: Long = 1L) extends FrameProducer {
  import RangeInput._

  def start(n: Long): RangeInput = copy(start = n)
  def end(n: Long): RangeInput = copy(end = n)
  def step(n: Long): RangeInput = copy(inc = n)

  protected def compute(implicit runtime: SparkRuntime): DataFrame =
    ctx.range(start, end, inc, sc.defaultParallelism)

  def toXml: Elem =
    <node>
      <start>{ start }</start>
      <end>{ end }</end>
      <step>{ inc }</step>
    </node>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag) ~ ("start" -> start) ~ ("end" -> end) ~ ("step" -> inc)
}

/**
 * Range Input companion object.
 */
object RangeInput {
  val tag = "range-input"

  def apply(): RangeInput = apply(0, 100, 1)

  def fromXml(xml: Node) = {
    val start = xml \ "start" asLong
    val end = xml \ "end" asLong
    val inc = (xml \ "step" getAsLong) getOrElse 1L

    apply(start, end, inc)
  }

  def fromJson(json: JValue) = {
    val start = json \ "start" asLong
    val end = json \ "end" asLong
    val inc = (json \ "step" getAsLong) getOrElse 1L

    apply(start, end, inc)
  }
}