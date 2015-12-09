package com.ignition.frame

import scala.io.Source
import scala.xml.{ Elem, Node, XML }

import org.apache.spark.sql.DataFrame
import org.json4s.{ jvalue2monadic, string2JsonInput }
import org.json4s.JValue
import org.json4s.JsonDSL.{ jobject2assoc, pair2Assoc, pair2jvalue, string2jvalue }
import org.json4s.jackson.JsonMethods.parse

import com.ignition.{ ConnectionSourceStub, ins, outs }
import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.RichNodeSeq

/**
 * Loads and executes a subflow stored in an external file (json or xml).
 */
case class Invoke(path: String, fileType: String = "json")
    extends FrameModule(Invoke.MAX_INPUTS, Invoke.MAX_OUTPUTS) {

  import Invoke._

  override val allInputsRequired = false

  protected def compute(args: IndexedSeq[DataFrame], index: Int, preview: Boolean)(implicit runtime: SparkRuntime): DataFrame = {
    val data = Source.fromFile(path).getLines mkString "\n"
    val step = if (fileType == "json")
      FrameStepFactory.fromJson(parse(data))
    else
      FrameStepFactory.fromXml(XML.loadString(data))

    args zip ins(step) foreach { case (arg, port) => port from ConnectionSourceStub(arg) }
    outs(step)(index).value(preview)
  }

  def toXml: Elem = <node><path type={ fileType }>{ path }</path></node>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag) ~ ("type" -> fileType) ~ ("path" -> path)
}

/**
 * Invoke companion object.
 */
object Invoke {
  val tag = "invoke"

  val MAX_INPUTS = 100
  val MAX_OUTPUTS = 100

  def fromXml(xml: Node) = {
    val path = xml \ "path" asString
    val fileType = xml \ "path" \ "@type" asString

    apply(path, fileType)
  }

  def fromJson(json: JValue) = {
    val path = json \ "path" asString
    val fileType = json \ "type" asString

    apply(path, fileType)
  }
}