package com.ignition.flow

import scala.xml.{ Elem, Node }

import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.types.StructType

import com.datastax.driver.core.{ BoundStatement, PreparedStatement, ProtocolVersion }
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.toRDDFunctions
import com.datastax.spark.connector.writer.{ RowWriter, RowWriterFactory }

/**
 * Cassandra row writer for DataFrame objects.
 */
case class DataRowWriter(schema: StructType, tableDef: TableDef) extends RowWriter[Row] {

  private val converters = schema map { field =>
    val column = tableDef.columnByName(field.name)
    column.columnType.converterToCassandra
  }

  private val estimatedSize = schema map { _.dataType.defaultSize } sum

  def bind(row: Row, stmt: PreparedStatement, protocolVersion: ProtocolVersion): BoundStatement = {
    val args = row.toSeq zip converters map {
      case (value, cnv) => cnv.convert(value)
    }
    stmt.bind(args: _*)
  }

  def estimateSizeInBytes(row: Row): Int = estimatedSize

  def columnNames: Seq[String] = schema.fieldNames
}
/**
 * Writes rows into a Cassandra table.
 *
 * @author Vlad Orzhekhovskiy
 */
case class CassandraOutput(keyspace: String, table: String) extends Transformer with XmlExport {

  implicit protected def rowWriterFactory(implicit ctx: SQLContext) =
    new RowWriterFactory[Row] {
      def rowWriter(table: TableDef, columnNames: Seq[String]) =
        new DataRowWriter(outSchema(0), table)
    }

  protected def compute(arg: DataFrame, limit: Option[Int])(implicit ctx: SQLContext): DataFrame = {
    val keyspace = this.keyspace
    val table = this.table
    val columns = SomeColumns(arg.columns: _*)
    val df = limit map arg.limit getOrElse arg
    df.rdd.saveToCassandra(keyspace, table, columns)
    df
  }

  protected def computeSchema(inSchema: StructType)(implicit ctx: SQLContext): StructType = inSchema

  def toXml: Elem = <cassandra-output keyspace={ keyspace } table={ table }/>

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}

/**
 * Cassandra Output companion object.
 */
object CassandraOutput {
  def fromXml(xml: Node): CassandraOutput = {
    val keyspace = (xml \ "@keyspace").text
    val table = (xml \ "@table").text
    CassandraOutput(keyspace, table)
  }
}