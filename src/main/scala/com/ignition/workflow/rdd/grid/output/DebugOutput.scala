package com.ignition.workflow.rdd.grid.output

import scala.xml.{ Elem, Node }

import org.apache.spark.rdd.RDD

import com.ignition.data.DataRow
import com.ignition.data.DataType.{ DoubleDataType, IntDataType }
import com.ignition.data.RowMetaData
import com.ignition.util.XmlUtils.{ RichNodeSeq, booleanToText, intToText, optToOptText }
import com.ignition.workflow.rdd.grid.{ GridStep1, XmlFactory }

/**
 * Prints out the data row to the standard output.
 *
 * @author Vlad Orzhekhovskiy
 */
case class DebugOutput(names: Boolean = true, types: Boolean = false, sampleSize: Option[Int] = None)
  extends GridStep1 {

  protected def computeRDD(rdd: RDD[DataRow]): RDD[DataRow] = {
    val meta = inMetaData.get
    val widths = calculateWidths(meta)
    val delimiter = widths map ("-" * _) mkString ("+", "+", "+")

    val dataTypes = meta.columns map (_.dataType)

    val typeLine = (dataTypes zip widths) map {
      case (dt, width) => s"%${width}s".format(dt.code)
    } mkString ("|", "|", "|")

    if (names || types) println(delimiter)

    if (names) {
      val nameLine = (meta.columnNames zip widths) map {
        case (name, width) => s"%${width}s".format(name)
      } mkString ("|", "|", "|")
      println(nameLine)
    }

    if (types)
      println(typeLine)

    println(delimiter)

    val data = sampleSize map rdd.take getOrElse rdd.collect

    data foreach { row =>
      val str = (dataTypes zipWithIndex) zip widths map {
        case ((IntDataType, index), width) => s"%${width}d".format(row.getInt(index))
        case ((DoubleDataType, index), width) => s"%${width}f".format(row.getDouble(index))
        case ((_, index), width) => s"%${width}s".format(row.getString(index).take(width))
      } mkString ("|", "|", "|")
      println(str)
    }
    println(delimiter)

    rdd
  }

  private def calculateWidths(meta: RowMetaData) = meta.columnNames map { name =>
    math.max(math.min(name.size, 15), 10)
  }

  def toXml: Elem = <debug-output names={ names } types={ types } size={ sampleSize }/>

  def outMetaData: Option[RowMetaData] = inMetaData

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}

/**
 * Debug output companion object.
 */
object DebugOutput extends XmlFactory[DebugOutput] {
  def fromXml(xml: Node) = {
    val names = (xml \ "@names").asBoolean
    val types = (xml \ "@types").asBoolean
    val sampleSize = (xml \ "@size").getAsInt
    DebugOutput(names, types, sampleSize)
  }
}