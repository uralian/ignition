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
case class Filter(condition: Column) extends FrameSplitter(2) {
  import Filter._

  protected def compute(arg: DataFrame, index: Int, limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
    val df = optLimit(arg, limit)
    val column = if (index == 0) condition else !condition
    df.filter(column)
  }

  protected def computeSchema(inSchema: StructType, index: Int)(implicit runtime: SparkRuntime): StructType = inSchema

  def toXml: Elem = <node><condition>{ condition.toString }</condition></node>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag) ~ ("condition" -> condition.toString)
}

/**
 * Filter companion object.
 */
object Filter {
  val tag = "filter"

  def apply(condition: String): Filter = apply(new Column(condition))

  def fromXml(xml: Node) = apply(xml \ "condition" asString)

  def fromJson(json: JValue) = apply(json \ "condition" asString)
}