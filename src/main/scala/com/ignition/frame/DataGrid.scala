package com.ignition.frame

import scala.xml.{ Elem, Node }
import scala.xml.NodeSeq.seqToNodeSeq

import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types.{ StructField, StructType }
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jvalue2monadic

import com.ignition.SparkRuntime
import com.ignition.types.TypeUtils._
import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.{ RichNodeSeq, booleanToText }

/**
 * Static data grid input.
 *
 * @author Vlad Orzhekhovskiy
 */
case class DataGrid(schema: StructType, rows: Seq[Row]) extends FrameProducer {
  import DataGrid._

  validate

  def addRow(row: Row): DataGrid = copy(rows = rows :+ row)

  def addRow(values: Any*): DataGrid = addRow(Row(values: _*))

  def rows(tuples: Product*): DataGrid = copy(rows = tuples map Row.fromTuple)

  protected def compute(preview: Boolean)(implicit runtime: SparkRuntime): DataFrame = {
    val data = if (preview) rows.take(FrameStep.previewSize) else rows
    val rdd = ctx.sparkContext.parallelize(data)
    ctx.createDataFrame(rdd, schema)
  }

  override protected def buildSchema(index: Int)(implicit runtime: SparkRuntime): StructType = schema

  def toXml: Elem =
    <node>
      { schemaToXml(schema) }
      <rows>
        {
          rows map { row =>
            <row>
              { 0 until row.size map (index => <item>{ valueToXml(row(index)) }</item>) }
            </row>
          }
        }
      </rows>
    </node>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag) ~ ("schema" -> schemaToJson(schema)) ~
    ("rows" -> rows.map(_.toSeq map valueToJson))

  /**
   * Validate:
   * - schema's column count == row column count
   * - schema(i) data type == row(i) data type
   * - if row(i) is null, schema(i) must be nullable
   */
  private def validate() = {
    rows foreach { row =>
      assert(row.size == schema.size, s"Schema column count ${} and row size do not match")
      (0 until schema.size) foreach { index =>
        if (row.isNullAt(index))
          assert(schema.fields(index).nullable, s"Null value in a non-nullable column: ${schema.fields(index).name}")
        else {
          val valueType = typeForValue(row(index))
          val schemaType = schema.fields(index).dataType
          assert(valueType == schemaType, s"Wrong data type: $valueType, must be $schemaType")
        }
      }
    }
  }
}

/**
 * Data grid companion object.
 */
object DataGrid {
  val tag = "datagrid"

  def apply(schema: StructType): DataGrid = apply(schema, Nil)

  def fromXml(xml: Node) = {
    val schema = xmlToSchema((xml \ "schema").head)
    val rows = (xml \\ "row") map { node =>
      val items = (schema.fields zip (node \ "item")) map {
        case (field, node) => xmlToValue(field.dataType, node.child)
      }
      Row.fromSeq(items)
    }
    apply(schema, rows)
  }

  def schemaToXml(schema: StructType) =
    <schema>
      {
        schema.fields map { field =>
          <field name={ field.name } type={ nameForType(field.dataType) } nullable={ field.nullable }/>
        }
      }
    </schema>

  def xmlToSchema(xml: Node) = {
    val fields = (xml \ "field") map { node =>
      val name = node \ "@name" asString
      val dataType = typeForName(node \ "@type" asString)
      val nullable = node \ "@nullable" asBoolean

      StructField(name, dataType, nullable)
    }
    StructType(fields)
  }

  def fromJson(json: JValue) = {
    val schema = jsonToSchema(json \ "schema")
    val rows = (json \ "rows" asArray) map { jr =>
      val items = schema zip jr.asArray map {
        case (field, value) => jsonToValue(field.dataType, value)
      }
      Row.fromSeq(items)
    }
    apply(schema, rows)
  }

  def schemaToJson(schema: StructType): JValue = schema.fields.toList map { field =>
    ("name" -> field.name) ~ ("type" -> nameForType(field.dataType)) ~ ("nullable" -> field.nullable)
  }

  def jsonToSchema(json: JValue) = {
    val fields = (json asArray) map { jf =>
      val name = jf \ "name" asString
      val dataType = typeForName(jf \ "type" asString)
      val nullable = jf \ "nullable" asBoolean

      StructField(name, dataType, nullable)
    }
    StructType(fields)
  }
}