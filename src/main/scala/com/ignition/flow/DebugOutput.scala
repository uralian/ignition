package com.ignition.flow

import scala.xml.{ Elem, Node }

import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.sql.types.{ DoubleType, IntegerType, StructType }

import com.ignition.util.XmlUtils.{ RichNodeSeq, booleanToText, intToText, optToOptText }

/**
 * Prints out data on the standard output.
 *
 * @author Vlad Orzhekhovskiy
 */
case class DebugOutput(names: Boolean = true, types: Boolean = false, sampleSize: Option[Int] = None) extends Transformer with XmlExport {

  protected def compute(arg: DataFrame)(implicit ctx: SQLContext): DataFrame = {
    val schema = inputSchemas(ctx)(0).get
    val widths = calculateWidths(schema)
    val delimiter = widths map ("-" * _) mkString ("+", "+", "+")

    val dataTypes = schema map (_.dataType)

    val typeLine = (dataTypes zip widths) map {
      case (dt, width) => s"%${width}s".format(dt.typeName)
    } mkString ("|", "|", "|")

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

    val data = sampleSize map arg.take getOrElse arg.collect

    data foreach { row =>
      val str = (dataTypes zipWithIndex) zip widths map {
        case ((IntegerType, index), width) => s"%${width}d".format(row.getInt(index))
        case ((DoubleType, index), width) => s"%${width}f".format(row.getDouble(index))
        case ((_, index), width) => s"%${width}s".format(row.getString(index).take(width))
      } mkString ("|", "|", "|")
      println(str)
    }
    println(delimiter)

    arg
  }

  protected def computeSchema(inSchema: Option[StructType])(implicit ctx: SQLContext): Option[StructType] =
    inSchema

  def toXml: Elem = <debug-output names={ names } types={ types } size={ sampleSize }/>

  private def calculateWidths(schema: StructType) = schema.fieldNames map { name =>
    math.max(math.min(name.length, 15), 10)
  }

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}

/**
 * Debug output companion object.
 */
object DebugOutput {
  def fromXml(xml: Node) = {
    val names = (xml \ "@names").asBoolean
    val types = (xml \ "@types").asBoolean
    val sampleSize = (xml \ "@size").getAsInt
    DebugOutput(names, types, sampleSize)
  }
}