package com.ignition.frame.mllib

import scala.xml.{ Elem, Node }

import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types.StructType
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jvalue2monadic

import com.ignition.SparkRuntime
import com.ignition.frame.FrameTransformer
import com.ignition.types.{ RichStructType, double, fieldToRichStruct, long }
import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.RichNodeSeq

/**
 * Calculates column-based statistics using MLLib library.
 *
 * @author Vlad Orzhekhovskiy
 */
case class ColumnStats(dataFields: Iterable[String], groupFields: Iterable[String] = Nil)
  extends FrameTransformer with MLFunctions {

  import ColumnStats._

  def add(fields: String*) = copy(dataFields = dataFields ++ fields)
  def %(fields: String*) = add(fields: _*)

  def groupBy(fields: String*) = copy(groupFields = fields)

  protected def compute(arg: DataFrame, preview: Boolean)(implicit runtime: SparkRuntime): DataFrame = {
    val df = optLimit(arg, preview)

    val rdd = toVectors(df, dataFields, groupFields)
    rdd.persist

    val keys = rdd.keys.distinct.collect
    val rows = keys map { key =>
      val slice = rdd filter (_._1 == key) values
      val st = Statistics.colStats(slice)

      val data = (0 until dataFields.size) flatMap { idx =>
        Seq(st.max(idx), st.min(idx), st.mean(idx), st.numNonzeros(idx),
          st.variance(idx), st.normL1(idx), st.normL2(idx))
      }
      Row.fromSeq((key.toSeq :+ st.count) ++ data)
    }

    val targetRDD = ctx.sparkContext.parallelize(rows)
    val targetFields = ((groupFields map df.schema.apply toSeq) :+ long("count")) ++
      dataFields.zipWithIndex.flatMap {
        case (name, idx) => double(s"${name}_max") ~ double(s"${name}_min") ~
          double(s"${name}_mean") ~ double(s"${name}_non0") ~ double(s"${name}_variance") ~
          double(s"${name}_normL1") ~ double(s"${name}_normL2")
      }
    val schema = StructType(targetFields)

    ctx.createDataFrame(targetRDD, schema)
  }

  def toXml: Elem =
    <node>
      <aggregate>
        {
          dataFields map { name => <field name={ name }/> }
        }
      </aggregate>
      {
        if (!groupFields.isEmpty)
          <group-by>
            { groupFields map (f => <field name={ f }/>) }
          </group-by>
      }
    </node>.copy(label = tag)

  def toJson: org.json4s.JValue = {
    val groupBy = if (groupFields.isEmpty) None else Some(groupFields)
    val aggregate = dataFields map (_.toString)
    ("tag" -> tag) ~ ("groupBy" -> groupBy) ~ ("aggregate" -> aggregate)
  }
}

/**
 * Columns Stats companion object.
 */
object ColumnStats {
  val tag = "column-stats"
  
  def apply(dataFields: String*): ColumnStats = apply(dataFields, Nil)

  def fromXml(xml: Node) = {
    val dataFields = (xml \ "aggregate" \ "field") map { _ \ "@name" asString }
    val groupFields = (xml \ "group-by" \ "field") map (_ \ "@name" asString)
    apply(dataFields, groupFields)
  }

  def fromJson(json: JValue) = {
    val dataFields = (json \ "aggregate" asArray) map (_ asString)
    val groupFields = (json \ "groupBy" asArray) map (_ asString)
    apply(dataFields, groupFields)
  }
}