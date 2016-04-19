package com.ignition.frame

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.xml.{ Elem, Node }

import org.apache.spark.sql.{ DataFrame, SaveMode }
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jvalue2monadic

import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.RichNodeSeq

/**
 * Writes data into a database table over JDBC.
 *
 * @author Vlad Orzhekhovskiy
 */
case class JdbcOutput(connUrl: String, table: String, saveMode: SaveMode = SaveMode.Append,
                      username: Option[String] = None, password: Option[String] = None,
                      properties: Map[String, String] = Map.empty) extends FrameTransformer {
  import JdbcOutput._

  def url(connUrl: String): JdbcOutput = copy(connUrl = connUrl)
  def table(tableName: String): JdbcOutput = copy(table = tableName)
  def mode(saveMode: SaveMode): JdbcOutput = copy(saveMode = saveMode)
  def username(user: String): JdbcOutput = copy(username = Some(user))
  def password(pwd: String): JdbcOutput = copy(password = Some(pwd))
  def properties(props: (String, String)*) = copy(properties = props.toMap)

  protected def compute(arg: DataFrame)(implicit runtime: SparkRuntime): DataFrame = {
    assert(connUrl != null, "JDBC URL not set")
    assert(table != null, "Table not set")

    val info = new java.util.Properties
    info.putAll(properties.asJava)
    username foreach (info.put("user", _))
    password foreach (info.put("password", _))

    val df = optLimit(arg, runtime.previewMode)

    df.write.mode(saveMode).jdbc(connUrl, table, info)
    df
  }

  def toXml: Elem =
    <node mode={ saveMode.name }>
      <url>{ connUrl }</url>
      <table>{ table }</table>
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
    ("tag" -> tag) ~ ("url" -> connUrl) ~ ("table" -> table) ~ ("mode" -> saveMode.name) ~
      ("username" -> username) ~ ("password" -> password) ~ ("properties" -> propJson)
  }
}

/**
 * JDBC Output companion object.
 */
object JdbcOutput {
  val tag = "jdbc-output"

  def apply(): JdbcOutput = apply(null, null)

  def fromXml(xml: Node) = {
    val url = xml \ "url" asString
    val table = xml \ "table" asString
    val saveMode = xml \ "@mode" asString
    val username = xml \ "username" getAsString
    val password = xml \ "password" getAsString
    val properties = xml \ "properties" \ "property" map { node =>
      val name = node \ "@name" asString
      val value = node.child.head asString

      name -> value
    } toMap

    apply(url, table, SaveMode.valueOf(saveMode), username, password, properties)
  }

  def fromJson(json: JValue) = {
    val url = json \ "url" asString
    val table = json \ "table" asString
    val saveMode = json \ "mode" asString
    val username = json \ "username" getAsString
    val password = json \ "password" getAsString
    val properties = (json \ "properties" asArray) map { item =>
      val name = item \ "name" asString
      val value = item \ "value" asString

      name -> value
    } toMap

    apply(url, table, SaveMode.valueOf(saveMode), username, password, properties)
  }
}