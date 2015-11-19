package com.ignition.stream

import scala.xml.{ Elem, Node }

import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jvalue2monadic

import com.ignition.SparkRuntime
import com.ignition.types.TypeUtils._
import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.{ RichNodeSeq, optToOptText, stringToText }

/**
 * Sets or drops the ignition runtime variables.
 *
 * @author Vlad Orzhekhovskiy
 */
case class SetVariables(vars: Map[String, Any]) extends StreamTransformer {
  import SetVariables._

  override val allInputsRequired: Boolean = false

  protected def compute(arg: DataStream, preview: Boolean)(implicit runtime: SparkRuntime): DataStream = {
    vars foreach {
      case (name, null) => runtime.vars.drop(name)
      case (name, value) => runtime.vars(name) = value
    }
    arg
  }

  def toXml: Elem =
    <node>
      {
        vars map {
          case (name, value) =>
            val dataType = Option(value) map (x => nameForType(typeForValue(x)))
            <var name={ name } type={ dataType }>{ valueToXml(value) }</var>
        }
      }
    </node>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag) ~ ("vars" -> vars.map {
    case (name, value) =>
      val dataType = Option(value) map (x => nameForType(typeForValue(x)))
      ("name" -> name) ~ ("type" -> dataType) ~ ("value" -> valueToJson(value))
  })
}

/**
 * SetVariables companion object.
 */
object SetVariables {
  val tag = "stream-set-variables"

  def apply(vars: (String, Any)*): SetVariables = apply(vars.toMap)

  def fromXml(xml: Node) = {
    val vars = xml \ "var" map { node =>
      val name = node \ "@name" asString
      val dataType = (node \ "@type" getAsString) map typeForName
      val value = dataType map (xmlToValue(_, node.child.head)) getOrElse null

      name -> value
    }
    apply(vars.toMap)
  }

  def fromJson(json: JValue) = {
    val vars = (json \ "vars" asArray) map { node =>
      val name = node \ "name" asString
      val dataType = (node \ "type" getAsString) map typeForName
      val value = dataType map (jsonToValue(_, node \ "value")) getOrElse null

      name -> value
    }
    apply(vars.toMap)
  }
}