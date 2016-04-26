package com.ignition.frame

import scala.xml.{ Elem, Node }

import org.apache.spark.sql.DataFrame
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jvalue2monadic

import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.RichNodeSeq

/**
 * Repartitions the underlying Spark RDD.
 */
case class Repartition(size: Int, shuffle: Boolean = true) extends FrameTransformer {
  import Repartition._

  def shuffle(flag: Boolean) = copy(shuffle = flag)

  protected def compute(arg: DataFrame)(implicit runtime: SparkRuntime): DataFrame = {
    val df = optLimit(arg, runtime.previewMode)
    if (shuffle) df.repartition(size) else df.coalesce(size)
  }

  def toXml: Elem = <node size={ size.toString } shuffle={ shuffle.toString }/>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag) ~ ("size" -> size) ~ ("shuffle" -> shuffle)
}

/**
 * Repartition companion object.
 */
object Repartition {
  val tag = "repartition"

  def fromXml(xml: Node) = {
    val size = xml \ "@size" asInt
    val shuffle = xml \ "@shuffle" asBoolean

    apply(size, shuffle)
  }

  def fromJson(json: JValue) = {
    val size = json \ "size" asInt
    val shuffle = json \ "shuffle" asBoolean

    apply(size, shuffle)
  }
}