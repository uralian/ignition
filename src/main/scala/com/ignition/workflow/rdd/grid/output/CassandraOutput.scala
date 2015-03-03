package com.ignition.workflow.rdd.grid.output

import scala.xml.{ Elem, Node }

import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

import com.datastax.driver.core.{ BoundStatement, PreparedStatement, ProtocolVersion }
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.toRDDFunctions
import com.datastax.spark.connector.writer.{ RowWriter, RowWriterFactory }
import com.eaio.uuid.UUID
import com.ignition.data.{ DataRow, RowMetaData }
import com.ignition.workflow.rdd.grid.{ GridStep1, XmlFactory }

/**
 * Writes rows into a Cassandra table.
 *
 * @author Vlad Orzhekhovskiy
 */
case class CassandraOutput(keyspace: String, table: String) extends GridStep1 {

  /**
   * Factory for DataRow writer.
   */
  implicit protected lazy val dataRowWriterFactory = new RowWriterFactory[DataRow] {
    def rowWriter(table: TableDef, columnNames: Seq[String]): RowWriter[DataRow] =
      new DataRowWriter(inMetaData.get.columnNames)
  }

  protected def computeRDD(rdd: RDD[DataRow]): RDD[DataRow] = {
    val keyspace = this.keyspace
    val table = this.table
    val columns = SomeColumns(inMetaData.get.columnNames: _*)
    rdd.saveToCassandra(keyspace, table, columns)
    rdd
  }

  val outMetaData: Option[RowMetaData] = inMetaData

  def toXml: Elem = <cassandra-output keyspace={ keyspace } table={ table }/>

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}

/**
 * Cassandra row writer for DataRow objects.
 */
case class DataRowWriter(columnNames: Seq[String]) extends RowWriter[DataRow] {

  def bind(data: DataRow, stmt: PreparedStatement, protocolVersion: ProtocolVersion): BoundStatement = {

    val args = data.rawData map {
      case x: UUID => java.util.UUID.fromString(x.toString)
      case x: DateTime => x.toDate
      case x: BigDecimal => x.bigDecimal
      case x @ _ => x
    } map (_.asInstanceOf[Object])
    stmt.bind(args: _*)
  }

  def estimateSizeInBytes(data: DataRow): Int = data.columnCount * 20
}

/**
 * Cassandra Output companion object.
 */
object CassandraOutput extends XmlFactory[CassandraOutput] {
  def fromXml(xml: Node): CassandraOutput = {
    val keyspace = (xml \ "@keyspace").text
    val table = (xml \ "@table").text
    CassandraOutput(keyspace, table)
  }
}