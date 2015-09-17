package com.ignition.stream

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
 * Filters the stream based on a combination of boolean conditions against fields.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Filter(condition: Column) extends StreamSplitter(2) {
  import Filter._

  val expressions = Array(toSQL(condition), "NOT (" + toSQL(condition) + ")")

  protected def compute(arg: DataStream, index: Int, limit: Option[Int])(implicit runtime: SparkRuntime): DataStream = {

    val expr = expressions(index)
    val filterFunc = (df: DataFrame) => df.filter(expr)

    transformAsDF(filterFunc)(arg)(runtime.ctx)
  }

  protected def computeSchema(inSchema: StructType, index: Int)(implicit runtime: SparkRuntime): StructType = inSchema

  def toXml: Elem = <node><condition>{ toSQL(condition) }</condition></node>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag) ~ ("condition" -> toSQL(condition))

  private def toSQL(column: Column) = column.toString.replace("&&", "and").replace("||", "or")
}

/**
 * Filter companion object.
 */
object Filter {
  val tag = "stream-filter"

  def apply(condition: String): Filter = apply(new Column(condition))

  def fromXml(xml: Node) = apply(xml \ "condition" asString)

  def fromJson(json: JValue) = apply(json \ "condition" asString)
}