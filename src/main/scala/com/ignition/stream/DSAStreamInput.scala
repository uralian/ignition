package com.ignition.stream

import scala.xml.{ Elem, Node }

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ DataType, MetadataBuilder, StructField, StructType }
import org.dsa.iot.spark.DSAReceiver
import org.json4s.JValue
import org.json4s.JsonDSL.{ pair2Assoc, seq2jvalue, string2jvalue }
import org.json4s.jvalue2monadic

import com.ignition.types.TypeUtils.{ nameForType, typeForName }
import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.RichNodeSeq

/**
 * Creates a stream from a collection of DSA paths.
 *
 * @author Vlad Orzhekhovskiy
 */
case class DSAStreamInput(paths: Iterable[(String, DataType)]) extends StreamProducer {
  import DSAStreamInput._

  def add(path: String, dt: DataType) = copy(paths = this.paths.toSeq :+ (path -> dt))
  def %(path: String, dt: DataType) = add(path, dt)

  def add(tuple: (String, DataType)) = copy(paths = this.paths.toSeq :+ tuple)
  def %(tuple: (String, DataType)) = add(tuple)

  private val fields = paths.zipWithIndex map {
    case ((path, dt), index) =>
      val name = pathToFieldName(path)
      val meta = new MetadataBuilder().putString("path", path).putLong("index", index).build
      StructField(name, dt, true, meta)
  }
  val schema = StructType(fields.toSeq)

  protected def compute(preview: Boolean)(implicit runtime: SparkStreamingRuntime): DataStream = {

    val stream = ssc.receiverStream(new DSAReceiver(paths.map(_._1).toSeq: _*))
    stream map {
      case (path, time, value) =>
        val data = new Array[Any](schema.length)
        val name = pathToFieldName(path)
        val index = schema(name).metadata.getLong("index").toInt
        data(index) = value
        new GenericRowWithSchema(data, schema).asInstanceOf[Row]
    }
  }

  def toXml: Elem =
    <node>
      <paths>
        { paths map (p => <path type={ nameForType(p._2) }>{ p._1 }</path>) }
      </paths>
    </node>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag) ~ ("paths" -> paths.map { p =>
    ("type" -> nameForType(p._2)) ~ ("path" -> p._1)
  })

  private def pathToFieldName(path: String) = {
    val index = path.lastIndexOf('/')
    if (index < 0) path else path.substring(index + 1)
  }
}

/**
 * DSA Stream Input companion object.
 */
object DSAStreamInput {
  val tag = "stream-dsa-input"

  def apply(paths: (String, String)*): DSAStreamInput = new DSAStreamInput(paths map { p =>
    p._1 -> typeForName(p._2)
  })

  def fromXml(xml: Node) = {
    val paths = (xml \ "paths" \ "path") map { node =>
      val path = node asString
      val dataType = typeForName(node \ "@type" asString)
      path -> dataType
    }
    apply(paths)
  }

  def fromJson(json: JValue) = {
    val paths = (json \ "paths" asArray) map { node =>
      val path = node \ "path" asString
      val dataType = typeForName(node \ "type" asString)
      path -> dataType
    }
    apply(paths)
  }
}