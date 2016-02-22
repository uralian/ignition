package com.ignition.frame

import scala.util.Try
import scala.util.control.NonFatal
import scala.xml.{ Elem, Node }

import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types.{ DataType, StringType, StructField, StructType }
import org.json4s.JValue
import org.json4s.JsonDSL.{ jobject2assoc, option2jvalue, pair2Assoc, pair2jvalue, string2jvalue }
import org.json4s.jvalue2monadic

import com.ignition.types.{ fieldToRichStruct, string }
import com.ignition.types.TypeUtils.valueOf
import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.RichNodeSeq

/**
 * Reads CSV files.
 *
 * @author Vlad Orzhekhovskiy
 */
case class CsvFileInput(path: String, separator: Option[String] = Some(","),
                        schema: Option[StructType] = None) extends FrameProducer {

  import CsvFileInput._

  def separator(sep: Option[String]): CsvFileInput = copy(separator = sep)
  def separator(sep: String): CsvFileInput = separator(Some(sep))

  def schema(schema: StructType): CsvFileInput = copy(schema = Some(schema))

  protected def compute(implicit runtime: SparkRuntime): DataFrame = {
    val path = injectGlobals(this.path)
    val separator = this.separator

    val lines = ctx.sparkContext.textFile(path)

    // if schema not defined, take first line, split by separator and derive schema
    val derivedSchema = schema getOrElse (lines.take(1) match {
      case Array(line: String) =>
        val arr = separator map line.split getOrElse Array(line)
        val fields = (0 until arr.length) map (idx => StructField(s"COL$idx", StringType))
        StructType(fields)
      case _ =>
        string("COL0").schema
    })

    val rdd = lines map { line =>
      val arr = separator map line.split getOrElse Array(line)
      val data = arr zip derivedSchema map {
        case (str, field) => convert(str, field.dataType, field.nullable)
      }
      Row.fromSeq(data)
    }

    val df = ctx.createDataFrame(rdd, derivedSchema)
    optLimit(df, runtime.previewMode)
  }

  def toXml: Elem =
    <node>
      <path>{ path }</path>
      { separator map (s => <separator>{ s }</separator>) toList }
      { schema map (s => DataGrid.schemaToXml(s)) toList }
    </node>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag) ~ ("path" -> path) ~ ("separator" -> separator) ~
    ("schema" -> (schema map DataGrid.schemaToJson))
}

/**
 * CSV Input companion object.
 */
object CsvFileInput {
  val tag = "csv-file-input"

  /**
   * Creates a new CsvFileInput with the specified separator.
   */
  def apply(path: String, separator: String): CsvFileInput = apply(path, Some(separator))

  /**
   * Creates a new CsvFileInput with the specified separator and schema.
   */
  def apply(path: String, separator: String, schema: StructType): CsvFileInput =
    apply(path, Some(separator), Some(schema))

  def fromXml(xml: Node) = {
    val path = xml \ "path" asString
    val separator = xml \ "separator" getAsString
    val schema = (xml \ "schema").headOption map DataGrid.xmlToSchema
    apply(path, separator, schema)
  }

  def fromJson(json: JValue) = {
    val path = json \ "path" asString
    val separator = json \ "separator" getAsString
    val schema = (json \ "schema").toOption map DataGrid.jsonToSchema
    apply(path, separator, schema)
  }

  /**
   * Parses a string and returns a value consistent with the specified data type.
   * If the value cannot be parsed and <tt>nullable</tt> flag is <code>true</code>,
   * it returns <code>null</null>, otherwise it throws a runtime exception.
   */
  def convert(str: String, dataType: DataType, nullable: Boolean) = Try {
    valueOf(str, dataType)
  } recover {
    case NonFatal(e) if nullable => null
    case NonFatal(e) => throw e
  } get
}