package com.ignition.flow

import java.sql.Date

import scala.xml.{ Elem, Node }
import scala.xml.NodeSeq.seqToNodeSeq

import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.types._

import com.datastax.spark.connector.{ CassandraRow, toSparkContextFunctions }
import com.datastax.spark.connector.types.TypeConverter
import com.ignition.types.TypeUtils.{ typeForName, typeForValue, valueToXml, xmlToValue }
import com.ignition.util.XmlUtils.RichNodeSeq

import com.ignition.SparkRuntime

/**
 * CQL WHERE clause.
 */
case class Where(cql: String, values: Any*) extends XmlExport {
  def toXml: Elem =
    <where>
      <cql>{ cql }</cql>
      { values map (v => <arg type={ typeForValue(v).typeName }>{ valueToXml(v) }</arg>) }
    </where>
}

/**
 * CQL WHERE companion object.
 */
object Where {
  /**
   * Converts XML into a WHERE clause.
   */
  def fromXml(xml: Node) = {
    val cql = (xml \ "cql").text
    val values = (xml \ "arg") map { node =>
      val dataType = typeForName((node \ "@type").asString)
      xmlToValue(dataType, node.child)
    }
    Where(cql, values: _*)
  }
}

/**
 * Reads rows from Apache Cassandra.
 *
 * @author Vlad Orzhekhovskiy
 */
case class CassandraInput(keyspace: String, table: String, schema: StructType,
  where: Option[Where]) extends Producer with XmlExport {
  
  def where(filter: Where) = copy(where = Some(filter))

  protected def compute(limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
    val schema = this.schema
    val dataTypes = schema.fields map (_.dataType)
    val cassRDD = ctx.sparkContext.cassandraTable[CassandraRow](keyspace, table).select(schema.fieldNames: _*)
    val rows = where map (w => cassRDD.where(w.cql, w.values: _*)) getOrElse cassRDD
    val limitedRows = limit map (n => ctx.sparkContext.parallelize(rows.take(n))) getOrElse rows
    val rdd = limitedRows map { cr =>
      @inline def convert[T](x: Any)(implicit tc: TypeConverter[T]) = tc.convert(x)
      val data = dataTypes zip cr.iterator.toArray[Any] map {
        case (BinaryType, x) => convert[Array[Byte]](x)
        case (BooleanType, x) => convert[Boolean](x)
        case (StringType, x) => convert[String](x)
        case (ByteType, x) => convert[Byte](x)
        case (ShortType, x) => convert[Short](x)
        case (IntegerType, x) => convert[Int](x)
        case (LongType, x) => convert[Long](x)
        case (FloatType, x) => convert[Float](x)
        case (DoubleType, x) => convert[Double](x)
        case (_: DecimalType, x) => Decimal(convert[BigDecimal](x)).toJavaBigDecimal
        case (DateType, x) => new java.sql.Date(convert[Date](x).getTime)
        case (TimestampType, x) => new java.sql.Timestamp(convert[Date](x).getTime)
        case (t @ _, _) => throw new IllegalArgumentException(s"Invalid data type: $t")
      }
      Row.fromSeq(data)
    }
    ctx.createDataFrame(rdd, schema)
  }

  protected def computeSchema(implicit runtime: SparkRuntime): StructType = schema

  def toXml: Elem =
    <cassandra-input keyspace={ keyspace } table={ table }>
      { DataGrid.schemaToXml(schema) }
      {
        where map (_.toXml) toList
      }
    </cassandra-input>

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}

/**
 * Cassandra Input companion object.
 */
object CassandraInput {
  def apply(keyspace: String, table: String, schema: StructType): CassandraInput =
    apply(keyspace, table, schema, None)

  def apply(keyspace: String, table: String, schema: StructType, cql: String, args: Any*): CassandraInput =
    apply(keyspace, table, schema, Some(Where(cql, args: _*)))

  def fromXml(xml: Node) = {
    val keyspace = (xml \ "@keyspace").text
    val table = (xml \ "@table").text
    val meta = DataGrid.xmlToSchema((xml \ "schema").head)
    val where = (xml \ "where").headOption map Where.fromXml
    CassandraInput(keyspace, table, meta, where)
  }
}