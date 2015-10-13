package com.ignition.stream

import scala.xml.{ Elem, Node }

import org.apache.spark.streaming.{ Duration, Milliseconds }
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jvalue2monadic

import com.ignition.SparkRuntime
import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.{ RichNodeSeq, longToText, optToOptText }

/**
 * Sliding time window.
 *
 * @author Vlad Orzhekhovskiy
 */

case class Window(windowDuration: Duration, slideDuration: Option[Duration] = None)
  extends StreamTransformer {

  import Window._

  protected def compute(arg: DataStream, preview: Boolean)(implicit runtime: SparkRuntime): DataStream =
    arg.window(windowDuration, slideDuration getOrElse arg.slideDuration)

  def toXml: Elem =
    <node duration={ windowDuration.milliseconds } slide={ slideDuration.map(_.milliseconds) }/>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag) ~ ("duration" -> windowDuration.milliseconds) ~
    ("slide" -> slideDuration.map(_.milliseconds))
}

/**
 * Sliding window companion object.
 *
 */
object Window {
  val tag = "stream-window"

  def apply(windowDuration: Duration, slideDuration: Duration): Window =
    apply(windowDuration, Some(slideDuration))
    
  def fromXml(xml: Node) = {
    val wd = Milliseconds(xml \ "@duration" asLong)
    val sd = (xml \ "@slide" getAsLong) map Milliseconds.apply
    
    apply(wd, sd)
  }
  
  def fromJson(json: JValue) = {
    val wd = Milliseconds(json \ "duration" asLong)
    val sd = (json \ "slide" getAsLong) map Milliseconds.apply
    
    apply(wd, sd)
  }
}