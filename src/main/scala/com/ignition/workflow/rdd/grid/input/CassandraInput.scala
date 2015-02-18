package com.ignition.workflow.rdd.grid.input

import scala.xml.{ Elem, Node }

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

import com.datastax.spark.connector.{ CassandraRow, toSparkContextFunctions }
import com.eaio.uuid.UUID
import com.ignition.data.{ Binary, DataRow, DataType }
import com.ignition.data.{ Decimal, DefaultRowMetaData, RowMetaData }
import com.ignition.data.DataType.{ BinaryDataType, BooleanDataType, DateTimeDataType, DecimalDataType, DoubleDataType, IntDataType, StringDataType, UUIDDataType }
import com.ignition.workflow.rdd.grid.{ GridStep0, XmlFactory }

/**
 * CQL WHERE clause.
 */
case class Where(cql: String, values: Any*) {
  private val str = implicitly[DataType[String]]

  val arguments = values map {
    case v: UUID => java.util.UUID.fromString(v.toString)
    case v @ _ => v
  }

  private def getType(x: Any): DataType[_] = x match {
    case Boolean => implicitly[DataType[Boolean]]
    case Byte => implicitly[DataType[Int]]
    case Short => implicitly[DataType[Int]]
    case Int => implicitly[DataType[Int]]
    case Long => implicitly[DataType[Decimal]]
    case Float => implicitly[DataType[Double]]
    case Double => implicitly[DataType[Double]]
    case BigDecimal => implicitly[DataType[Decimal]]
    case _: DateTime => implicitly[DataType[DateTime]]
    case _: java.util.Date => implicitly[DataType[DateTime]]
    case _: UUID => implicitly[DataType[UUID]]
    case _: java.util.UUID => implicitly[DataType[UUID]]
    case _: Array[Byte] => implicitly[DataType[Binary]]
    case _ => implicitly[DataType[String]]
  }

  def toXml: Elem =
    <where>
      <cql>{ cql }</cql>
      { values map (v => <arg type={ getType(v).code }>{ str.convert(v) }</arg>) }
    </where>
}

/**
 * CQL WHERE companion object.
 */
object Where extends XmlFactory[Where] {
  /**
   * Restores Where from XML.
   */
  def fromXml(xml: Node) = {
    val cql = (xml \ "cql").text
    val values = (xml \ "arg") map { node =>
      val code = (node \ "@type").text
      DataType.withCode(code).convert(node.text)
    }
    Where(cql, values: _*)
  }
}

/**
 * Reads rows from Apache Cassandra.
 *
 * @author Vlad Orzhekhovskiy
 */
case class CassandraInput(keyspace: String, table: String, meta: RowMetaData,
  where: Option[Where]) extends GridStep0 {

  protected def computeRDD(implicit sc: SparkContext): RDD[DataRow] = {
    val rows = sc.cassandraTable[CassandraRow](keyspace, table).select(meta.columnNames: _*)
    val rdd = where map (w => rows.where(w.cql, w.arguments: _*)) getOrElse rows
    rdd map { row =>
      val values = meta.columns map { ci =>
        ci.dataType match {
          case BooleanDataType => row.getBooleanOption(ci.name) getOrElse null
          case StringDataType => row.getStringOption(ci.name) getOrElse null
          case IntDataType => row.getIntOption(ci.name) getOrElse null
          case DoubleDataType => row.getDoubleOption(ci.name) getOrElse null
          case DecimalDataType => row.getDecimalOption(ci.name) getOrElse null
          case DateTimeDataType => row.getDateTimeOption(ci.name) getOrElse null
          case UUIDDataType => row.getUUIDOption(ci.name) map (id => new UUID(id.toString)) getOrElse null
          case BinaryDataType => row.getBytesOption(ci.name) map (_.array) getOrElse null
        }
      }
      meta.row(values)
    }
  }

  val outMetaData: Option[RowMetaData] = Some(meta)

  def toXml: Elem =
    <cassandra-input keyspace={ keyspace } table={ table }>
      { meta.toXml }
      {
        where map (_.toXml) toList
      }
    </cassandra-input>
}

/**
 * Cassandra Input companion object.
 */
object CassandraInput extends XmlFactory[CassandraInput] {
  def apply(keyspace: String, table: String, meta: RowMetaData): CassandraInput =
    apply(keyspace, table, meta, None)

  def apply(keyspace: String, table: String, meta: RowMetaData, cql: String, args: Any*): CassandraInput =
    apply(keyspace, table, meta, Some(Where(cql, args: _*)))

  def fromXml(xml: Node) = {
    val keyspace = (xml \ "@keyspace").text
    val table = (xml \ "@table").text
    val meta = DefaultRowMetaData.fromXml((xml \ "meta").head)
    val where = (xml \ "where").headOption map Where.fromXml
    CassandraInput(keyspace, table, meta, where)
  }
}