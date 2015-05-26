package com.ignition.flow

import scala.xml.{Elem, Node}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import com.ignition.{SparkRuntime, XmlExport}
import com.ignition.util.XmlUtils.{RichNodeSeq, booleanToText}

/**
 * Prints out data on the standard output.
 *
 * @author Vlad Orzhekhovskiy
 */
case class DebugOutput(names: Boolean = true, types: Boolean = false) extends FlowTransformer with XmlExport {

  protected def compute(arg: DataFrame, limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
    val schema = arg.schema
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

    val data = limit map arg.take getOrElse arg.collect

    data foreach { row =>
      val str = (dataTypes zip row.toSeq zip widths) map {
        case ((BinaryType, obj), width) =>
          s"%${width}s".format(obj.asInstanceOf[Array[Byte]].map("%02X" format _).mkString)
        case ((ByteType, obj), width) => s"%${width}d".format(obj)
        case ((ShortType, obj), width) => s"%${width}d".format(obj)
        case ((IntegerType, obj), width) => s"%${width}d".format(obj)
        case ((LongType, obj), width) => s"%${width}d".format(obj)
        case ((FloatType, obj), width) => s"%${width}f".format(obj)
        case ((DoubleType, obj), width) => s"%${width}f".format(obj)
        case ((_: DecimalType, obj), width) =>
          s"%${width}s".format(obj.asInstanceOf[java.math.BigDecimal].toPlainString)
        case ((_, obj), width) => s"%${width}s".format(obj.toString.take(width))
      } mkString ("|", "|", "|")
      println(str)
    }
    println(delimiter)

    arg
  }

  protected def computeSchema(inSchema: StructType)(implicit runtime: SparkRuntime): StructType = inSchema

  def toXml: Elem = <debug-output names={ names } types={ types }/>

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
    DebugOutput(names, types)
  }
}