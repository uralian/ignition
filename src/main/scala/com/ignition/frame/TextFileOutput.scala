package com.ignition.frame

import java.io.PrintWriter

import scala.xml.{ Elem, Node }

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jvalue2monadic

import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.{ RichNodeSeq, booleanToText }

/**
 * Writes rows to a CSV file.
 *
 * @author Vlad Orzhekhovskiy
 */
case class TextFileOutput(path: String, fields: Iterable[(String, String)],
                          separator: String = ",", outputHeader: Boolean = true) extends FrameTransformer {

  import TextFileOutput._

  def add(f: (String, String)*): TextFileOutput = copy(fields = fields.toSeq ++ f)
  def %(f: (String, String)*): TextFileOutput = add(f: _*)

  def add(name: String, format: String = "%s"): TextFileOutput = copy(fields = fields.toSeq :+ name -> format)
  def %(name: String, format: String = "%s"): TextFileOutput = add(name, format)

  def separator(sep: String): TextFileOutput = copy(separator = sep)
  def header(out: Boolean): TextFileOutput = copy(outputHeader = out)

  protected def compute(arg: DataFrame, preview: Boolean)(implicit runtime: SparkRuntime): DataFrame = {
    val path = injectGlobals(this.path)
    val out = new PrintWriter(path)

    if (outputHeader) {
      val header = fields map (_._1) mkString separator
      out.println(header)
    }

    val columns = fields map (ff => arg.col(ff._1)) toSeq

    val fmts = fields map (_._2) zipWithIndex

    val df = optLimit(arg, preview)
    df.select(columns: _*).collect foreach { row =>
      val line = fmts map {
        case (fmt, index) => fmt.format(row(index))
      } mkString separator
      out.println(line)
    }

    out.close

    df
  }

  override protected def buildSchema(index: Int)(implicit runtime: SparkRuntime): StructType =
    input(true).schema

  def toXml: Elem =
    <node outputHeader={ outputHeader }>
      <path>{ path }</path>
      <fields>
        {
          fields map (f => <field name={ f._1 }>{ f._2 }</field>)
        }
      </fields>
      <separator>{ separator }</separator>
    </node>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag) ~ ("path" -> path) ~ ("separator" -> separator) ~
    ("outputHeader" -> outputHeader) ~
    ("fields" -> fields.map(f => ("name" -> f._1) ~ ("format" -> f._2)))
}

/**
 * CSV output companion object.
 */
object TextFileOutput {
  val tag = "text-file-output"

  def apply(filename: String, formats: (String, String)*): TextFileOutput = apply(filename, formats)

  def fromXml(xml: Node) = {
    val path = xml \ "path" asString
    val separator = xml \ "separator" asString
    val outputHeader = xml \ "@outputHeader" asBoolean
    val formats = xml \ "fields" \ "field" map { node =>
      val name = node \ "@name" asString
      val format = node.child.head asString

      name -> format
    }
    apply(path, formats, separator, outputHeader)
  }

  def fromJson(json: JValue) = {
    val path = json \ "path" asString
    val separator = json \ "separator" asString
    val outputHeader = json \ "outputHeader" asBoolean
    val formats = (json \ "fields" asArray) map { node =>
      val name = node \ "name" asString
      val format = node \ "format" asString

      name -> format
    }
    apply(path, formats, separator, outputHeader)
  }
}