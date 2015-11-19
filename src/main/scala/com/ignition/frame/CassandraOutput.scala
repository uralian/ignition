package com.ignition.frame

import scala.xml.{ Elem, Node }
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types.StructType
import org.json4s.JValue
import org.json4s.JsonDSL.{ jobject2assoc, pair2Assoc, pair2jvalue, string2jvalue }
import org.json4s.jvalue2monadic
import com.datastax.driver.core.{ BoundStatement, PreparedStatement, ProtocolVersion }
import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.toRDDFunctions
import com.datastax.spark.connector.writer.{ RowWriter, RowWriterFactory }
import com.ignition.SparkRuntime
import com.ignition.util.JsonUtils.RichJValue
import com.datastax.spark.connector.ColumnRef

/**
 * Cassandra row writer for DataFrame objects.
 */
case class DataRowWriter(schema: StructType, tableDef: TableDef) extends RowWriter[Row] {

  private val converters = schema map { field =>
    val column = tableDef.columnByName(field.name)
    column.columnType.converterToCassandra
  }

  def columnNames: Seq[String] = schema.fieldNames

  def readColumnValues(data: Row, buffer: Array[Any]) = {
    val args = data.toSeq zip converters map {
      case (value, cnv) => cnv.convert(value)
    }
    args.zipWithIndex foreach {
      case (arg, index) => buffer(index) = arg
    }
  }
}

/**
 * Writes rows into a Cassandra table.
 *
 * @author Vlad Orzhekhovskiy
 */
case class CassandraOutput(keyspace: String, table: String) extends FrameTransformer {

  import CassandraOutput._

  implicit protected def rowWriterFactory(implicit runtime: SparkRuntime) =
    new RowWriterFactory[Row] {
      def rowWriter(table: TableDef, selectedColumns: IndexedSeq[ColumnRef]) =
        new DataRowWriter(buildSchema(0), table)
    }

  protected def compute(arg: DataFrame, preview: Boolean)(implicit runtime: SparkRuntime): DataFrame = {
    val keyspace = this.keyspace
    val table = this.table
    val columns = SomeColumns(arg.columns map (s => s: ColumnRef): _*)
    val df = optLimit(arg, preview)
    df.rdd.saveToCassandra(keyspace, table, columns)
    df
  }

  override protected def buildSchema(index: Int)(implicit runtime: SparkRuntime): StructType =
    input(true).schema

  def toXml: Elem = <node keyspace={ keyspace } table={ table }/>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag) ~ ("keyspace" -> keyspace) ~ ("table" -> table)
}

/**
 * Cassandra Output companion object.
 */
object CassandraOutput {
  val tag = "cassandra-output"

  def fromXml(xml: Node): CassandraOutput = {
    val keyspace = (xml \ "@keyspace").text
    val table = (xml \ "@table").text

    CassandraOutput(keyspace, table)
  }

  def fromJson(json: JValue): CassandraOutput = {
    val keyspace = json \ "keyspace" asString
    val table = json \ "table" asString

    CassandraOutput(keyspace, table)
  }
}