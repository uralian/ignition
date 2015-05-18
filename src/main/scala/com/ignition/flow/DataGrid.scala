package com.ignition.flow

import scala.xml.{ Elem, Node }
import scala.xml.NodeSeq.seqToNodeSeq

import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.types.{ StructField, StructType }

import com.ignition.types.TypeUtils.{ typeForName, typeForValue, valueToXml, xmlToValue }
import com.ignition.util.XmlUtils.{ RichNodeSeq, booleanToText }

import com.ignition.SparkRuntime

/**
 * Static data grid input.
 *
 * @author Vlad Orzhekhovskiy
 */
case class DataGrid(schema: StructType, rows: Seq[Row]) extends Producer with XmlExport {
  import DataGrid._

  validate

  def addRow(row: Row): DataGrid = copy(rows = rows :+ row)

  def addRow(values: Any*): DataGrid = addRow(Row(values: _*))

  def rows(tuples: Any*): DataGrid = {
    val rs = tuples map {
      case p : Product => Row.fromTuple(p)
      case v => Row(v)
    }
    copy(rows = rs)
  }

  protected def compute(limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
    val data = limit map rows.take getOrElse rows
    val rdd = ctx.sparkContext.parallelize(data)
    ctx.createDataFrame(rdd, schema)
  }

  protected def computeSchema(implicit runtime: SparkRuntime): StructType = schema
  
  def toXml: Elem =
    <datagrid>
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
    </datagrid>

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
          assert(schema(index).nullable, s"Null value in a non-nullable column: ${schema(index).name}")
        else {
          val valueType = typeForValue(row(index))
          val schemaType = schema(index).dataType
          assert(valueType == schemaType, s"Wrong data type: $valueType, must be $schemaType")
        }
      }
    }
  }

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}

/**
 * Data grid companion object.
 */
object DataGrid {

  def apply(schema: StructType): DataGrid = apply(schema, Nil)

  def fromXml(xml: Node) = {
    val schema = xmlToSchema((xml \ "schema").head)
    val rows = (xml \\ "row") map { node =>
      val items = (schema.fields zip (node \ "item")) map {
        case (field, node) => xmlToValue(field.dataType, node.child)
      }
      Row.fromSeq(items)
    }
    DataGrid(schema, rows)
  }

  def schemaToXml(schema: StructType) =
    <schema>
      {
        schema.fields map { field =>
          <field name={ field.name } type={ field.dataType.typeName } nullable={ field.nullable }/>
        }
      }
    </schema>

  def xmlToSchema(xml: Node) = {
    val fields = (xml \ "field") map { node =>
      val name = (node \ "@name").asString
      val dataType = typeForName((node \ "@type").asString)
      val nullable = (node \ "@nullable").asBoolean
      StructField(name, dataType, nullable)
    }
    StructType(fields)
  }
}