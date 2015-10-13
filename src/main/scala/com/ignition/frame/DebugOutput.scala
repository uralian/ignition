package com.ignition.frame

import scala.xml.{ Elem, Node }
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jvalue2monadic

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.ignition.SparkRuntime
import com.ignition.util.XmlUtils.{ RichNodeSeq, booleanToText, intToText, optToOptText }
import com.ignition.util.JsonUtils._
import scala.annotation.tailrec

/**
 * Prints out data on the standard output.
 *
 * @author Vlad Orzhekhovskiy
 */
case class DebugOutput(names: Boolean = true, types: Boolean = false,
                       title: Option[String] = None, maxWidth: Option[Int] = Some(80))
  extends FrameTransformer {

  import DebugOutput._

  def showNames(names: Boolean): DebugOutput = copy(names = names)
  def showTypes(types: Boolean): DebugOutput = copy(types = types)

  def title(title: String): DebugOutput = copy(title = Some(title))
  def noTitle(): DebugOutput = copy(title = None)

  def maxWidth(width: Int): DebugOutput = copy(maxWidth = Some(width))
  def unlimitedWidth(): DebugOutput = copy(maxWidth = None)

  protected def compute(arg: DataFrame, preview: Boolean)(implicit runtime: SparkRuntime): DataFrame = {
    val schema = arg.schema
    val data = if (preview) arg.take(FrameStep.previewSize) else arg.collect

    val widths = calculateWidths(schema, data)
    val delimiter = widths map ("-" * _) mkString ("+", "+", "+")

    val dataTypes = schema map (_.dataType)

    val typeLine = (dataTypes zip widths) map {
      case (dt, width) => s"%${width}s".format(dt.typeName)
    } mkString ("|", "|", "|")

    title foreach println

    if (names || types) println(delimiter)

    if (names) {
      val nameLine = (schema.fieldNames zip widths) map {
        case (name, width) => s"%${width}s".format(name)
      } mkString ("|", "|", "|")
      println(nameLine)
    }

    if (types)
      println(typeLine)

    println(delimiter)

    def formatValue(tuple: (Any, DataType, Int)) = tuple match {
      case (obj, BinaryType, width) =>
        s"%${width}s".format(obj.asInstanceOf[Array[Byte]].map("%02X" format _).mkString)
      case (obj, ByteType, width) => s"%${width}d".format(obj)
      case (obj, ShortType, width) => s"%${width}d".format(obj)
      case (obj, IntegerType, width) => s"%${width}d".format(obj)
      case (obj, LongType, width) => s"%${width}d".format(obj)
      case (obj, FloatType, width) => s"%${width}f".format(obj)
      case (obj, DoubleType, width) => s"%${width}f".format(obj)
      case (obj, _: DecimalType, width) =>
        s"%${width}s".format(obj.asInstanceOf[java.math.BigDecimal].toPlainString)
      case (obj, _, width) => s"%${width}s".format(obj.toString.take(width))
    }

    data foreach { row =>
      val str = (row.toSeq zip dataTypes zip widths) map {
        case ((obj, dt), width) => (obj, dt, width)
      } map { tuple =>
        formatValue(tuple).take(tuple._3)
      } mkString ("|", "|", "|")
      println(str)
    }
    println(delimiter)

    arg
  }

  override protected def buildSchema(index: Int)(implicit runtime: SparkRuntime): StructType =
    input(true).schema

  def toXml: Elem =
    <node names={ names } types={ types } max-width={ maxWidth }>
      { title map (t => <title>{ t }</title>) toList }
    </node>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag) ~ ("names" -> names) ~ ("types" -> types) ~
    ("maxWidth" -> maxWidth) ~ ("title" -> title)

  private def calculateWidths(schema: StructType, data: Array[Row]) = {

    @tailrec
    def normalize(widths: Seq[Int]): Seq[Int] = {
      val total = widths.sum + widths.size + 1
      if (total > maxWidth.getOrElse(0))
        normalize(widths.map(_ * 9 / 10))
      else
        widths
    }

    val widths = (schema zipWithIndex) map {
      case (field, idx) =>
        val nameWidth = if (names) field.name.size else 0
        val typeWidth = if (types) field.dataType.typeName.size else 0
        val valueWidths = data map { row => Option(row(idx)) map (_.toString.size) getOrElse 0 }
        val maxValueWidth = valueWidths.max
        List(nameWidth, typeWidth, maxValueWidth, 2).max
    }

    normalize(widths)
  }
}

/**
 * Debug output companion object.
 */
object DebugOutput {
  val tag = "debug-output"

  def fromXml(xml: Node) = {
    val names = xml \ "@names" asBoolean
    val types = xml \ "@types" asBoolean
    val title = xml \ "title" getAsString
    val maxWidth = xml \ "@max-width" getAsInt

    apply(names, types, title, maxWidth)
  }

  def fromJson(json: JValue) = {
    val names = json \ "names" asBoolean
    val types = json \ "types" asBoolean
    val title = json \ "title" getAsString
    val maxWidth = json \ "maxWidth" getAsInt

    apply(names, types, title, maxWidth)
  }
}