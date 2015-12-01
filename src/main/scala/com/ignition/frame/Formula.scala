package com.ignition.frame

import scala.xml.{ Elem, Node }

import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types.{ StructField, StructType, DataType }
import org.json4s.JValue
import org.json4s.JsonDSL.{ pair2Assoc, pair2jvalue, seq2jvalue, string2jvalue }
import org.json4s.jackson.JsonMethods.render
import org.json4s.jvalue2monadic

import com.ignition.script.RowExpression
import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.RichNodeSeq

/**
 * Calculates new fields based on string expressions in various dialects.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Formula(fields: Iterable[(String, RowExpression[_ <: DataType])]) extends FrameTransformer {
  import Formula._

  def addField(name: String, expr: RowExpression[_ <: DataType]) = copy(fields = fields.toSeq :+ (name -> expr))
  def %(name: String, expr: RowExpression[_ <: DataType]) = addField(name, expr)

  protected def compute(arg: DataFrame, preview: Boolean)(implicit runtime: SparkRuntime): DataFrame = {
    val df = optLimit(arg, preview)

    val executors = fields map {
      case (_, expr) => expr.evaluate(df.schema) _
    }

    val rdd = df map { row =>
      val computed = executors map (_(row))
      Row.fromSeq(row.toSeq ++ computed)
    }
    ctx.createDataFrame(rdd, computeSchema)
  }
  
  override protected def buildSchema(index: Int)(implicit runtime: SparkRuntime): StructType = computeSchema

  private def computeSchema(implicit runtime: SparkRuntime): StructType = {
    val df = input(true)
    val inSchema = df.schema
    val newFields = fields map {
      case (name, expr) =>
        val targetType = expr.targetType getOrElse expr.computeTargetType(inSchema)(df.first)
        StructField(name, targetType, true)
    }
    StructType(inSchema ++ newFields)
  }

  def toXml: Elem =
    <node>{ fields map (f => <field name={ f._1 }>{ f._2.toXml }</field>) }</node>.copy(label = tag)

  def toJson: JValue =
    ("tag" -> tag) ~ ("fields" -> fields.map {
      case (name, expr) => render("name" -> name) merge expr.toJson
    })
}

/**
 * Formula companion object.
 */
object Formula {
  val tag = "formula"

  def apply(fields: (String, RowExpression[_ <: DataType])*): Formula = apply(fields)

  def fromXml(xml: Node) = {
    val fields = xml \ "field" map { node =>
      val name = node \ "@name" asString
      val expr = RowExpression.fromXml(node.child.head)
      name -> expr
    }
    apply(fields)
  }

  def fromJson(json: JValue) = {
    val fields = (json \ "fields" asArray) map { item =>
      val name = item \ "name" asString
      val expr = RowExpression.fromJson(item)
      name -> expr
    }
    apply(fields)
  }
}