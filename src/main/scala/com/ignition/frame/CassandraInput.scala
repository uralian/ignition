package com.ignition.frame

import scala.xml.{ Elem, Node }
import scala.xml.NodeSeq.seqToNodeSeq

import org.apache.spark.sql.{ DataFrame, Row, types => sql }
import org.apache.spark.sql.types.{ StructField, StructType }
import org.json4s.{ JArray, JField, JObject, JString, JValue }
import org.json4s.JsonDSL.{ jobject2assoc, option2jvalue, pair2Assoc, pair2jvalue, seq2jvalue, string2jvalue }
import org.json4s.jvalue2monadic

import com.datastax.spark.connector.CassandraRow
import com.datastax.spark.connector.rdd.CassandraRDD
import com.datastax.spark.connector.toSparkContextFunctions
import com.datastax.spark.connector.types._
import com.ignition.SparkRuntime
import com.ignition.types.TypeUtils.{ jsonToValue, typeForName, typeForValue, valueToJson, valueToXml, xmlToValue }
import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.RichNodeSeq

/**
 * CQL WHERE clause.
 */
case class Where(cql: String, values: Any*) {

  def toXml: Elem =
    <where>
      <cql>{ cql }</cql>
      { values map (v => <arg type={ typeForValue(v).typeName }>{ valueToXml(v) }</arg>) }
    </where>

  def toJson: JValue = ("cql" -> cql) ~ ("args" -> values.map { v =>
    ("type" -> typeForValue(v).typeName) ~ ("value" -> valueToJson(v))
  })
}

/**
 * CQL WHERE companion object.
 */
object Where {

  def fromXml(xml: Node) = {
    val cql = (xml \ "cql").text
    val values = (xml \ "arg") map { node =>
      val dataType = typeForName((node \ "@type").asString)
      xmlToValue(dataType, node.child)
    }
    Where(cql, values: _*)
  }

  def fromJson(json: JValue) = (for {
    JObject(fields) <- json
    JField("cql", JString(cql)) <- fields
    JField("args", JArray(args)) <- fields
  } yield {
    val values = for {
      JObject(arg) <- args
      JField("type", JString(argType)) <- arg
      JField("value", JString(argValue)) <- arg
    } yield {
      val dataType = typeForName(argType)
      jsonToValue(dataType, argValue)
    }
    Where(cql, values: _*)
  }).head
}

/**
 * Reads rows from Apache Cassandra.
 *
 * @author Vlad Orzhekhovskiy
 */
case class CassandraInput(keyspace: String, table: String, columns: Iterable[String] = Nil,
                          where: Option[Where] = None) extends FrameProducer {

  import CassandraInput._

  def columns(cols: String*): CassandraInput = copy(columns = cols)
  def %(cols: String*) = columns(cols: _*)

  def where(cql: String, values: Any*): CassandraInput = copy(where = Some(Where(cql, values: _*)))

  protected def compute(limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
    val tableRDD = ctx.sparkContext.cassandraTable[CassandraRow](keyspace, table)
    val selectRDD = if (columns.isEmpty) tableRDD else tableRDD.select(columns.toSeq: _*)
    val schema = createSchema(selectRDD)
    val rows = where map (w => selectRDD.where(w.cql, w.values: _*)) getOrElse selectRDD
    val limitedRows = limit map (n => ctx.sparkContext.parallelize(rows.take(n))) getOrElse rows

    val dataTypes = schema.fields map (_.dataType)
    val rdd = limitedRows map { cr =>
      @inline def convert[T](x: Any)(implicit tc: TypeConverter[T]) = tc.convert(x)
      val data = dataTypes zip cr.iterator.toSeq map {
        case (_, null) => null
        case (sql.BinaryType, x) => convert[Array[Byte]](x)
        case (sql.BooleanType, x) => convert[Boolean](x)
        case (sql.StringType, x) => convert[String](x)
        case (sql.ByteType, x) => convert[Byte](x)
        case (sql.ShortType, x) => convert[Short](x)
        case (sql.IntegerType, x) => convert[Int](x)
        case (sql.LongType, x) => convert[Long](x)
        case (sql.FloatType, x) => convert[Float](x)
        case (sql.DoubleType, x) => convert[Double](x)
        case (_: sql.DecimalType, x) => convert[java.math.BigDecimal](x)
        case (sql.DateType, x) => new java.sql.Date(convert[java.util.Date](x).getTime)
        case (sql.TimestampType, x) => new java.sql.Timestamp(convert[java.util.Date](x).getTime)
        case (t @ _, _) => throw new IllegalArgumentException(s"Invalid data type: $t")
      }
      Row.fromSeq(data)
    }

    ctx.createDataFrame(rdd, schema)
  }

  protected def computeSchema(implicit runtime: SparkRuntime): StructType = computedSchema(0)

  def toXml: Elem =
    <node keyspace={ keyspace } table={ table }>
      {
        if (!columns.isEmpty)
          <columns>{ columns map (c => <column name={ c }/>) }</columns>
      }
      {
        where map (_.toXml) toList
      }
    </node>.copy(label = tag)

  def toJson: JValue = {
    val cols = if (columns.isEmpty) None else Some(columns)
    ("tag" -> tag) ~ ("keyspace" -> keyspace) ~ ("table" -> table) ~
      ("columns" -> cols) ~ ("where" -> where.map(_.toJson))
  }

  private def createSchema(rdd: CassandraRDD[CassandraRow]) = {
    val tableDef = rdd.tableDef
    val fields = rdd.selectedColumnNames map (tableDef.columnByName) map { cd =>
      val dataType = columnTypeToSqlType(cd.columnType)
      StructField(cd.columnName, dataType, !cd.isPrimaryKeyColumn)
    }
    StructType(fields)
  }
}

/**
 * Cassandra Input companion object.
 */
object CassandraInput {
  val tag = "cassandra-input"

  def apply(keyspace: String, table: String, columns: String*): CassandraInput =
    apply(keyspace, table, columns)

  def apply(keyspace: String, table: String, columns: Iterable[String], cql: String, args: Any*): CassandraInput =
    apply(keyspace, table, columns, Some(Where(cql, args: _*)))

  def fromXml(xml: Node) = {
    val keyspace = (xml \ "@keyspace").text
    val table = (xml \ "@table").text
    val columns = (xml \ "columns" \ "column") map (_ \ "@name" asString)
    val where = (xml \ "where").headOption map Where.fromXml
    apply(keyspace, table, columns, where)
  }

  def fromJson(json: JValue) = {
    val keyspace = json \ "keyspace" asString
    val table = json \ "table" asString
    val columns = (json \ "columns" asArray) map (_ asString)
    val where = (json \ "where").toOption map Where.fromJson
    apply(keyspace, table, columns, where)
  }

  private[CassandraInput] def columnTypeToSqlType(ct: ColumnType[_]): sql.DataType = ct match {
    case TextType | AsciiType | VarCharType | InetType | UUIDType | TimeUUIDType => sql.StringType
    case IntType => sql.IntegerType
    case BigIntType | CounterType => sql.LongType
    case FloatType => sql.FloatType
    case DoubleType => sql.DoubleType
    case BooleanType => sql.BooleanType
    case VarIntType | DecimalType => sql.DecimalType.Unlimited
    case TimestampType => sql.TimestampType
    case BlobType => sql.BinaryType
  }
}