package com.ignition.frame

import scala.xml.{ Elem, Node }

import org.apache.spark.sql.DataFrame
import org.json4s.JValue
import org.json4s.JsonDSL.{ pair2jvalue, string2jvalue }

/**
 * Persists the data to avoid subsequent recomputation.
 */
case class Cache() extends FrameTransformer {
  import Cache._

  protected def compute(arg: DataFrame)(implicit runtime: SparkRuntime): DataFrame = {
    val df = optLimit(arg, runtime.previewMode)
    df.persist
    df
  }

  def toXml: Elem = <node/>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag)
}

/**
 * Cache companion object.
 */
object Cache {
  val tag = "cache"

  def fromXml(xml: Node) = apply()

  def fromJson(json: JValue) = apply()
}