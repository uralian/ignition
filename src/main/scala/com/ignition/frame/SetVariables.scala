package com.ignition.frame

import scala.xml.{ Elem, Node }

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.json4s.JValue
import org.json4s.JsonDSL.{ jobject2assoc, option2jvalue, pair2Assoc, seq2jvalue, string2jvalue }
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
case class SetVariables(vars: Map[String, Any]) extends FrameTransformer {
  import SetVariables._

  override val allInputsRequired: Boolean = false

  protected def compute(arg: DataFrame, preview: Boolean)(implicit runtime: SparkRuntime): DataFrame = {
    vars foreach {
      case (name, null) => runtime.vars.drop(name)
      case (name, value) => runtime.vars(name) = value
    }
    optLimit(arg, preview)
  }

  override protected def buildSchema(index: Int)(implicit runtime: SparkRuntime): StructType = 
    input(true).schema

  def toXml: Elem =
    <node>
      {
        vars map {
          case (name, value) =>
            val dataType = Option(value) map (typeForValue(_).typeName)
            <var name={ name } type={ dataType }>{ valueToXml(value) }</var>
        }
      }
    </node>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag) ~ ("vars" -> vars.map {
    case (name, value) =>
      val dataType = Option(value) map (typeForValue(_).typeName)
      ("name" -> name) ~ ("type" -> dataType) ~ ("value" -> valueToJson(value))
  })
}

/**
 * SetVariables companion object.
 */
object SetVariables {
  val tag = "set-variables"

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