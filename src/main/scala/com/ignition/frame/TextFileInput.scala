package com.ignition.frame

import scala.io.Source
import scala.xml.{ Elem, Node }

import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types.StructType
import org.json4s.JValue
import org.json4s.JsonDSL.{ jobject2assoc, option2jvalue, pair2Assoc, pair2jvalue, string2jvalue }
import org.json4s.jvalue2monadic

import com.ignition.SparkRuntime
import com.ignition.types.{ fieldToStructType, string }
import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.RichNodeSeq

/**
 * Reads the text file into a data frame with a single column.
 * If the separator is specified, splits the file into multiple rows, otherwise
 * the data frame will contain only one row.
 *
 * @author Vlad Orzhekhovskiy
 */
case class TextFileInput(path: String, separator: Option[String] = None, dataField: String = "content")
  extends FrameProducer {

  import TextFileInput._

  def separator(sep: String): TextFileInput = copy(separator = Some(sep))
  def field(f: String): TextFileInput = copy(dataField = f)

  val schema: StructType = string(dataField)
  val regex = separator map (_.r)

  protected def compute(preview: Boolean)(implicit runtime: SparkRuntime): DataFrame = {
    val path = injectGlobals(this.path)

    val src = Source.fromFile(path)
    val lines = try src.getLines.mkString("\n") finally src.close

    val fragments = regex map (_.split(lines)) getOrElse Array(lines)
    val rdd = sc.parallelize(fragments map (Row(_)))

    val df = ctx.createDataFrame(rdd, schema)
    optLimit(df, false)
  }

  override protected def buildSchema(index: Int)(implicit runtime: SparkRuntime): StructType = schema

  def toXml: Elem =
    <node>
      <path>{ path }</path>
      { separator map (s => <separator>{ s }</separator>) toList }
      <dataField>{ dataField }</dataField>
    </node>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag) ~ ("path" -> path) ~ ("separator" -> separator) ~ ("dataField" -> dataField)
}

/**
 * Text File Input companion object.
 */
object TextFileInput {
  val tag = "text-file-input"

  def fromXml(xml: Node) = {
    val path = xml \ "path" asString
    val separator = xml \ "separator" getAsString
    val dataField = xml \ "dataField" asString

    apply(path, separator, dataField)
  }

  def fromJson(json: JValue) = {
    val path = json \ "path" asString
    val separator = json \ "separator" getAsString
    val dataField = json \ "dataField" asString

    apply(path, separator, dataField)
  }
}