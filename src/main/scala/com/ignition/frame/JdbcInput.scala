package com.ignition.frame

import java.sql.{ DriverManager, ResultSetMetaData }
import java.sql.Types._
import java.util.UUID

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.mutable.ArrayBuffer
import scala.xml.{ Elem, Node }

import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types._
import org.json4s.{ JObject, JValue }
import org.json4s.JsonDSL._
import org.json4s.jvalue2monadic

import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.RichNodeSeq

/**
 * Reads data from a JDBC source.
 *
 * @author Vlad Orzhekhovskiy
 */
case class JdbcInput(connUrl: String, sql: String,
                     username: Option[String] = None, password: Option[String] = None,
                     properties: Map[String, String] = Map.empty) extends FrameProducer {
  import JdbcInput._

  def url(connUrl: String): JdbcInput = copy(connUrl = connUrl)
  def sql(sql: String): JdbcInput = copy(sql = sql)
  def username(user: String): JdbcInput = copy(username = Some(user))
  def password(pwd: String): JdbcInput = copy(password = Some(pwd))
  def properties(props: (String, String)*) = copy(properties = props.toMap)

  protected def compute(implicit runtime: SparkRuntime): DataFrame = {
    assert(connUrl != null, "JDBC URL not set")
    assert(sql != null, "SQL query not set")

    val info = new java.util.Properties
    info.putAll(properties.asJava)
    username foreach (info.put("user", _))
    password foreach (info.put("password", _))

    val conn = DriverManager.getConnection(connUrl, info)

    val stmt = conn.createStatement
    val rs = stmt.executeQuery(sql)
    val schema = createSchema(rs.getMetaData)

    val columnCount = rs.getMetaData.getColumnCount
    val buffer = ArrayBuffer.empty[Row]
    while (rs.next && (!runtime.previewMode || buffer.size < FrameStep.previewSize)) {
      val data = (1 to columnCount) map rs.getObject map jdbcToAny
      buffer += Row.fromSeq(data)
    }
    rs.close

    val rdd = sc.parallelize(buffer)
    ctx.createDataFrame(rdd, schema)
  }

  private def jdbcToAny(x: Object): Any = x match {
    case null    => null
    case _: UUID => x.toString
    case _       => x
  }

  private def createSchema(metaData: ResultSetMetaData) = {
    val fields = (1 to metaData.getColumnCount) map { index =>
      val name = metaData.getColumnLabel(index)
      val dataType = jdbcTypeToSparkType(metaData.getColumnType(index))
      val nullable = metaData.isNullable(index) != 0
      StructField(name, dataType, nullable)
    }
    StructType(fields)
  }

  def toXml: Elem =
    <node>
      <url>{ connUrl }</url>
      <sql>{ sql }</sql>
      { username map (u => <username>{ u }</username>) toList }
      { password map (p => <password>{ p }</password>) toList }
      {
        if (!properties.isEmpty)
          <properties>
            {
              properties map {
                case (name, value)=> <property name={ name }>{ value }</property>
              }
            }
          </properties>
      }
    </node>.copy(label = tag)

  def toJson: JValue = {
    val propJson = if (properties.isEmpty) None else Some(properties map {
      case (name, value) => ("name" -> name) ~ ("value" -> value)
    })
    ("tag" -> tag) ~ ("url" -> connUrl) ~ ("sql" -> sql) ~ ("username" -> username) ~
      ("password" -> password) ~ ("properties" -> propJson)
  }
}

/**
 * JDBC Input companion object.
 */
object JdbcInput {
  val tag = "jdbc-input"

  def apply(): JdbcInput = apply(null, null)

  def fromXml(xml: Node) = {
    val url = xml \ "url" asString
    val sql = xml \ "sql" asString
    val username = xml \ "username" getAsString
    val password = xml \ "password" getAsString
    val properties = xml \ "properties" \ "property" map { node =>
      val name = node \ "@name" asString
      val value = node.child.head asString

      name -> value
    } toMap

    apply(url, sql, username, password, properties)
  }

  def fromJson(json: JValue) = {
    val url = json \ "url" asString
    val sql = json \ "sql" asString
    val username = json \ "username" getAsString
    val password = json \ "password" getAsString
    val properties = (json \ "properties" asArray) map { item =>
      val name = item \ "name" asString
      val value = item \ "value" asString

      name -> value
    } toMap

    apply(url, sql, username, password, properties)
  }

  private[JdbcInput] def jdbcTypeToSparkType(jdbcType: Int): DataType = jdbcType match {
    case BIT | BINARY | VARBINARY | LONGVARBINARY     => BinaryType
    case BLOB                                         => BinaryType
    case TINYINT                                      => ByteType
    case SMALLINT                                     => ShortType
    case INTEGER                                      => IntegerType
    case BIGINT                                       => LongType
    case FLOAT                                        => FloatType
    case REAL | DOUBLE                                => DoubleType
    case NUMERIC | DECIMAL                            => DecimalType.SYSTEM_DEFAULT
    case CHAR | VARCHAR | LONGVARCHAR | CLOB          => StringType
    case NCHAR | NVARCHAR | LONGNVARCHAR | NCLOB      => StringType
    case DATE                                         => DateType
    case TIME | TIMESTAMP                             => TimestampType
    case TIME_WITH_TIMEZONE | TIMESTAMP_WITH_TIMEZONE => TimestampType
    case BOOLEAN                                      => BooleanType
  }
}