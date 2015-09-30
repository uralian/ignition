package com.ignition.frame

import scala.xml.{ Elem, Node, NodeSeq }

import org.apache.spark.sql.{ Column, DataFrame}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructType
import org.json4s.JValue
import org.json4s.JsonDSL.{ jobject2assoc, pair2Assoc, seq2jvalue, string2jvalue }
import org.json4s.jvalue2monadic

import com.ignition.SparkRuntime
import com.ignition.types.TypeUtils._
import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.{ RichNodeSeq, stringToText }

/**
 * Adds a set of new fields, each of them either a constant, a variable, or an environment variable.
 * The value supplied for each new field can be of any supported data types; if it is a String,
 * it will also be checked against v{...} and e{...} patterns to inject Spark and JVM variables.
 *
 * @author Vlad Orzhekhovskiy
 */
case class AddFields(fields: Iterable[(String, Any)]) extends FrameTransformer {
  import AddFields._

  def add(name: String, value: Any) = copy(fields = this.fields.toSeq :+ (name -> value))
  def %(name: String, value: Any) = add(name, value)

  def add(tuple: (String, Any)) = copy(fields = this.fields.toSeq :+ tuple)
  def %(tuple: (String, Any)) = add(tuple)

  protected def compute(arg: DataFrame, limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
    val df = optLimit(arg, limit)

    def column(name: String, expr: Any) = lit(expr).as(name)

    val newColumns = fields map {
      case (name, VarLiteral(varName)) => column(name, runtime.vars(varName))
      case (name, EnvLiteral(envName)) => column(name, System.getProperty(envName))
      case (name, x @ _) => column(name, x)
    }

    val allColumns = List(new Column("*")) ++ newColumns
    df.select(allColumns: _*)
  }

  protected def computeSchema(inSchema: StructType)(implicit runtime: SparkRuntime): StructType =
    computedSchema(0)

  def toXml: Elem = {
    def xmlField(name: String, dType: String, value: NodeSeq) = {
      <field name={ name } type={ dType }>{ value }</field>
    }
    <node>
      {
        fields map {
          case (name, VarLiteral(varName)) => xmlField(name, "var", varName)
          case (name, EnvLiteral(envName)) => xmlField(name, "env", envName)
          case (name, x @ _) => xmlField(name, typeForValue(x).typeName, valueToXml(x))
        }
      }
    </node>.copy(label = tag)
  }

  def toJson: JValue = {
    def jsonValue(name: String, dType: String, value: JValue) = {
      ("name" -> name) ~ ("type" -> dType) ~ ("value" -> value)
    }
    ("tag" -> tag) ~ ("fields" -> fields.map {
      case (name, VarLiteral(varName)) => jsonValue(name, "var", varName)
      case (name, EnvLiteral(envName)) => jsonValue(name, "env", envName)
      case (name, x @ _) => jsonValue(name, typeForValue(x).typeName, valueToJson(x))
    })
  }
}

/**
 * Add Fields companion object.
 */
object AddFields {
  val tag = "add-fields"

  def apply(fields: (String, Any)*): AddFields = apply(fields)

  def fromXml(xml: Node) = {
    val fields = xml \ "field" map { node =>
      val name = node \ "@name" asString
      val typeStr = node \ "@type" asString
      val xValue = node.child.head
      val value = typeStr match {
        case "env" => EnvLiteral(xValue asString)
        case "var" => VarLiteral(xValue asString)
        case t @ _ => xmlToValue(typeForName(t), xValue)
      }
      name -> value
    }
    apply(fields)
  }

  def fromJson(json: JValue) = {
    val fields = (json \ "fields" asArray) map { node =>
      val name = node \ "name" asString
      val typeStr = node \ "type" asString
      val jValue = node \ "value"
      val value = typeStr match {
        case "env" => EnvLiteral(jValue asString)
        case "var" => VarLiteral(jValue asString)
        case t @ _ => jsonToValue(typeForName(t), jValue)
      }
      name -> value
    }
    apply(fields)
  }
}