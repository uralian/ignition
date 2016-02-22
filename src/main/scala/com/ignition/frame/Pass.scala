package com.ignition.frame

import scala.xml.{ Elem, Node }

import org.apache.spark.sql.DataFrame
import org.json4s.JValue
import org.json4s.JsonDSL.{ pair2jvalue, string2jvalue }

/**
 * A simple passthrough.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Pass() extends FrameTransformer {
  import Pass._

  protected def compute(arg: DataFrame)(implicit runtime: SparkRuntime) =
    optLimit(arg, runtime.previewMode)

  def toXml: Elem = <node/>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag)
}

/**
 * Passthrough companion object.
 */
object Pass {
  val tag = "pass"

  def fromXml(xml: Node) = apply()

  def fromJson(json: JValue) = apply()
}