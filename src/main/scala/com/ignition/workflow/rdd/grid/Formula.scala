package com.ignition.workflow.rdd.grid

import scala.xml._

import org.apache.spark.rdd.RDD

import com.ignition.data.{ ColumnInfo, DataRow, DefaultDataRow, DefaultRowMetaData, RowMetaData }
import com.ignition.scripting.RowExProcessor

/**
 * Calculates new fields based on string expressions in various dialects.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Formula(val fields: Map[String, RowExProcessor[_]]) extends GridStep1 {

  protected def computeRDD(rdd: RDD[DataRow]): RDD[DataRow] = {
    val newColumnNames = inMetaData.get.columnNames ++ fields.keys
    val meta = inMetaData
    val procs = fields.values.map(_.evaluate(meta) _)
    rdd map { row =>
      val newData = row.rawData ++ (procs map { _.apply(row) })
      DefaultDataRow(newColumnNames, newData)
    }
  }

  def outMetaData: Option[RowMetaData] = inMetaData map { in =>
    val newColumns = in.columns ++ fields.map {
      case (name, proc) => ColumnInfo(name)(proc.targetType)
    }
    DefaultRowMetaData(newColumns)
  }

  def toXml: Elem =
    <formula>
      {
        fields map {
          case (name, proc) => <field name={ name }>{ proc.toXml }</field>
        }
      }
    </formula>

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}

/**
 * Fomula companion object.
 */
object Formula extends XmlFactory[Formula] {
  def apply(fields: (String, RowExProcessor[_])*): Formula = new Formula(fields.toMap)

  def fromXml(xml: Node) = {
    val fields = (xml \ "field") map { node =>
      val name = (node \ "@name").text
      val processor = RowExProcessor.fromXml(node)
      (name, processor)
    }
    apply(fields: _*)
  }
}