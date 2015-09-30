package com.ignition.frame

import scala.xml.{ Elem, Node }

import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types.StructType
import org.json4s.JValue
import org.json4s.JsonDSL.{ jobject2assoc, pair2Assoc, pair2jvalue, string2jvalue }
import org.json4s.jvalue2monadic

import com.ignition.SparkRuntime
import com.ignition.types.{ fieldToRichStruct, string }
import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.RichNodeSeq

/**
 * Reads a folder of text files.
 *
 * @author Vlad Orzhekhovskiy
 */
case class TextFolderInput(path: String, nameField: String = "filename",
                           dataField: String = "content") extends FrameProducer {

  import TextFolderInput._

  def nameField(field: String): TextFolderInput = copy(nameField = field)
  def dataField(field: String): TextFolderInput = copy(dataField = field)

  val schema = string(nameField) ~ string(dataField)

  protected def compute(limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
    val path = injectGlobals(this.path)

    val rdd = ctx.sparkContext.wholeTextFiles(path) map {
      case (fileName, contents) => Row(fileName, contents)
    }
    val df = ctx.createDataFrame(rdd, schema)
    optLimit(df, limit)
  }

  protected def computeSchema(implicit runtime: SparkRuntime): StructType = schema

  def toXml: Elem = <node path={ path } nameField={ nameField } dataField={ dataField }/>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag) ~ ("path" -> path) ~ ("nameField" -> nameField) ~ ("dataField" -> dataField)
}

/**
 * Text Folder Input companion object.
 */
object TextFolderInput {
  val tag = "text-folder-input"

  def fromXml(xml: Node) = {
    val path = xml \ "@path" asString
    val nameField = xml \ "@nameField" asString
    val dataField = xml \ "@dataField" asString

    apply(path, nameField, dataField)
  }

  def fromJson(json: JValue) = {
    val path = json \ "path" asString
    val nameField = json \ "nameField" asString
    val dataField = json \ "dataField" asString

    apply(path, nameField, dataField)
  }
}