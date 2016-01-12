package com.ignition.frame

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.{ Duration, MILLISECONDS }
import scala.xml.{ Elem, Node }

import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ DataType, MetadataBuilder, StructField, StructType }
import org.dsa.iot.spark.{ DSAConnector, DSAHelper }
import org.json4s.JValue
import org.json4s.JsonDSL.{ pair2Assoc, seq2jvalue, string2jvalue }
import org.json4s.jvalue2monadic

import com.ignition.types.TypeUtils.{ nameForType, typeForName }
import com.ignition.util.ConfigUtils.{ RichConfig, getConfig }
import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.RichNodeSeq
import com.typesafe.config.ConfigException

/**
 * Reads values from DSA nodes.
 */
case class DSAInput(paths: Iterable[(String, DataType)]) extends FrameProducer {
  import DSAInput._

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

  private def pathToFieldName(path: String) = {
    val index = path.lastIndexOf('/')
    if (index < 0) path else path.substring(index + 1)
  }

  protected def compute(preview: Boolean)(implicit runtime: SparkRuntime): DataFrame = {
    implicit val requester = DSAConnector.requesterLink.getRequester
    val futures = paths.map(_._1).toSet map DSAHelper.getNodeValue
    val valueMap = Await.result(Future.sequence(futures), DSAInput.maxTimeout) map { v =>
      pathToFieldName(v._1) -> v._3
    } toMap

    val data = schema map (f => valueMap(f.name))
    val row = new GenericRowWithSchema(data.toArray, schema).asInstanceOf[Row]
    val rdd = ctx.sparkContext.parallelize(Seq(row))
    ctx.createDataFrame(rdd, schema)
  }

  override protected def buildSchema(index: Int)(implicit runtime: SparkRuntime): StructType = schema

  def toXml: Elem =
    <node>
      <paths>
        { paths map (p => <path type={ nameForType(p._2) }>{ p._1 }</path>) }
      </paths>
    </node>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag) ~ ("paths" -> paths.map { p =>
    ("type" -> nameForType(p._2)) ~ ("path" -> p._1)
  })
}

/**
 * DSA Input companion object.
 */
object DSAInput {
  val tag = "dsa-input"

  val maxTimeout = {
    val millis = getConfig("dsa").getTimeInterval("max-timeout").getMillis
    Duration.apply(millis, MILLISECONDS)
  }

  def apply(paths: (String, String)*): DSAInput = new DSAInput(paths map { p =>
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