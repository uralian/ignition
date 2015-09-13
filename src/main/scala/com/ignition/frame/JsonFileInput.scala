package com.ignition.frame

import scala.xml.{ Elem, Node }

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.json4s.JValue
import org.json4s.JsonDSL.{ jobject2assoc, pair2Assoc, pair2jvalue, seq2jvalue, string2jvalue }
import org.json4s.jvalue2monadic

import com.ignition.SparkRuntime
import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.RichNodeSeq

/**
 * Reads a JSON file, which contains a separate JSON object in each line.
 *
 * @author Vlad Orzhekhovskiy
 */
case class JsonFileInput(path: String, columns: Iterable[(String, String)]) extends FrameProducer {
  import JsonFileInput._

  def add(tuples: (String, String)*): JsonFileInput = copy(columns = this.columns ++ tuples)
  def %(tuples: (String, String)*): JsonFileInput = add(tuples: _*)

  protected def compute(limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
    val path = injectGlobals(this.path)

    val df = ctx.jsonFile(path)
    val cols = columns map {
      case (name, path) => df.col(path).as(name)
    }

    val result = df.select(cols.toSeq: _*)
    optLimit(result, limit)
  }

  protected def computeSchema(implicit runtime: SparkRuntime): StructType = computedSchema(0)

  def toXml: Elem =
    <node>
      <path>{ path }</path>
      { columns map (c => <field name={ c._1 }>{ c._2 }</field>) }
    </node>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag) ~ ("path" -> path) ~ ("fields" -> columns.map { c =>
    ("name" -> c._1) ~ ("xpath" -> c._2)
  })
}

/**
 * JSON file input companion object.
 */
object JsonFileInput {
  val tag = "json-file-input"

  def apply(path: String, columns: (String, String)*): JsonFileInput = apply(path, columns.toSeq)

  def fromXml(xml: Node) = {
    val path = xml \ "path" asString
    val columns = xml \ "field" map { node =>
      val name = node \ "@name" asString
      val xpath = node.child.head asString

      name -> xpath
    }
    apply(path, columns)
  }
  
  def fromJson(json: JValue) = {
    val path = json \ "path" asString
    val columns = (json \ "fields" asArray) map { item =>
      val name = item \ "name" asString
      val xpath = item \ "xpath" asString
      
      name -> xpath
    }
    apply(path, columns)
  }
}