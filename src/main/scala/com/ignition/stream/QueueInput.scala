package com.ignition.stream

import scala.collection.mutable.Queue
import scala.xml.{ Elem, Node }
import scala.xml.NodeSeq.seqToNodeSeq

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.json4s.JValue
import org.json4s.JsonDSL.{ jobject2assoc, pair2Assoc, pair2jvalue, seq2jvalue, string2jvalue }
import org.json4s.jvalue2monadic

import com.ignition.SparkRuntime
import com.ignition.frame.DataGrid
import com.ignition.types.TypeUtils.{ jsonToValue, valueToJson, valueToXml, xmlToValue }
import com.ignition.util.JsonUtils.RichJValue

/**
 * Creates a stream from a static data set, passing one RDD at a time.
 *
 * @author Vlad Orzhekhovskiy
 */
case class QueueInput(schema: StructType, data: Seq[Seq[Row]] = Nil) extends StreamProducer {
  import QueueInput._

  val dataWithSchema = data map { rows =>
    rows map { row =>
      new GenericRowWithSchema(row.toSeq.toArray, schema).asInstanceOf[Row]
    }
  }

  def addBatch(rdd: Seq[Row]) = copy(data = this.data :+ rdd)

  def addRows(tuples: Any*) = {
    val rs = tuples map {
      case p: Product => Row.fromTuple(p)
      case v => Row(v)
    }
    copy(data = this.data :+ rs)
  }

  protected def compute(preview: Boolean)(implicit runtime: SparkRuntime): DataStream = {
    val rdds = dataWithSchema map (sc.parallelize(_))

    val queue = Queue(rdds: _*)
    ssc.queueStream(queue, true)
  }

  def toXml: Elem =
    <node>
      { DataGrid.schemaToXml(schema) }
      <data>
        {
          data map { batch =>
            <batch>
              {
                batch map { row =>
                  <row>
                    { 0 until row.size map (index => <item>{ valueToXml(row(index)) }</item>) }
                  </row>
                }
              }
            </batch>
          }
        }
      </data>
    </node>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag) ~ ("schema" -> DataGrid.schemaToJson(schema)) ~
    ("data" -> data.map(_ map (_.toSeq map valueToJson)))
}

/**
 * Queue Input companion object.
 */
object QueueInput {
  val tag = "stream-queue-input"

  def fromXml(xml: Node) = {
    val schema = DataGrid.xmlToSchema((xml \ "schema").head)
    val data = (xml \\ "batch") map { node =>
      val rows = (node \\ "row") map { node =>
        val items = (schema.fields zip (node \ "item")) map {
          case (field, node) => xmlToValue(field.dataType, node.child)
        }
        Row.fromSeq(items)
      }
      rows
    }
    apply(schema, data)
  }

  def fromJson(json: JValue) = {
    val schema = DataGrid.jsonToSchema(json \ "schema")
    val data = (json \ "data" asArray) map { jd =>
      val rows = (jd asArray) map { jr =>
        val items = schema zip jr.asArray map {
          case (field, value) => jsonToValue(field.dataType, value)
        }
        Row.fromSeq(items)
      }
      rows
    }
    apply(schema, data)
  }
}