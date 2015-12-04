package com.ignition.stream

import scala.xml.{ Elem, Node }

import org.dsa.iot.spark.DSAConnector
import org.json4s.JValue
import org.json4s.JsonDSL.{ pair2Assoc, seq2jvalue, string2jvalue }
import org.json4s.jvalue2monadic

import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.RichNodeSeq

/**
 * Posts updates to DSA node tree.
 */
case class DSAStreamOutput(fields: Iterable[(String, String)]) extends StreamTransformer {
  import DSAStreamOutput._

  def add(name: String, path: String) = copy(fields = this.fields.toSeq :+ (name -> path))
  def %(name: String, path: String) = add(name, path)

  def add(tuple: (String, String)) = copy(fields = this.fields.toSeq :+ tuple)
  def %(tuple: (String, String)) = add(tuple)

  protected def compute(arg: DataStream, preview: Boolean)(implicit runtime: SparkStreamingRuntime): DataStream = {
    arg foreachRDD (_ foreach { row =>
      fields foreach {
        case (name, path) => DSAConnector.updateNode(path, row.getAs[Any](name))
      }
    })
    arg
  }

  def toXml: Elem =
    <node>
      <fields>
        { fields map (f => <field name={ f._1 }>{ f._2 }</field>) }
      </fields>
    </node>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag) ~ ("fields" -> fields.map { f =>
    ("name" -> f._1) ~ ("path" -> f._2)
  })
}

/**
 * DSA StreamOutput companion object.
 */
object DSAStreamOutput {
  val tag = "stream-dsa-output"

  def apply(fields: (String, String)*): DSAStreamOutput = new DSAStreamOutput(fields)

  def fromXml(xml: Node) = {
    val fields = (xml \ "fields" \ "field") map { node =>
      val name = node \ "@name" asString
      val path = node asString

      name -> path
    }
    apply(fields)
  }

  def fromJson(json: JValue) = {
    val fields = (json \ "fields" asArray) map { node =>
      val name = node \ "name" asString
      val path = node \ "path" asString

      name -> path
    }
    apply(fields)
  }
}