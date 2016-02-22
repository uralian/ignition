package com.ignition.frame.mllib

import scala.xml.{ Elem, Node }

import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types.StructType
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jvalue2monadic

import com.ignition.frame.{ FrameTransformer, SparkRuntime }
import com.ignition.types.double
import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.RichNodeSeq

import CorrelationMethod.{ CorrelationMethod, PEARSON }

/**
 * Correlation methods.
 */
object CorrelationMethod extends Enumeration {
  type CorrelationMethod = Value

  val PEARSON = Value("pearson")
  val SPEARMAN = Value("spearman")
}

/**
 * Computes the correlation between data series using MLLib library.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Correlation(dataFields: Iterable[String], groupFields: Iterable[String] = Nil,
                       method: CorrelationMethod = PEARSON) extends FrameTransformer with MLFunctions {

  import Correlation._

  def add(fields: String*) = copy(dataFields = dataFields ++ fields)
  def %(fields: String*) = add(fields: _*)

  def groupBy(fields: String*) = copy(groupFields = fields)
  def method(method: CorrelationMethod) = copy(method = method)

  protected def compute(arg: DataFrame)(implicit runtime: SparkRuntime): DataFrame = {
    val df = optLimit(arg, runtime.previewMode)

    val rdd = toVectors(df, dataFields, groupFields)
    rdd.persist

    val keys = rdd.keys.distinct.collect
    val rows = keys map { key =>
      val slice = rdd filter (_._1 == key) values
      val matrix = Statistics.corr(slice)

      val data = for {
        rowIdx <- 0 until dataFields.size
        colIdx <- rowIdx + 1 until dataFields.size
      } yield matrix(rowIdx, colIdx)
      Row.fromSeq(key.toSeq ++ data)
    }

    val targetRDD = ctx.sparkContext.parallelize(rows)
    val fieldSeq = dataFields.toSeq
    val targetFields = (groupFields map df.schema.apply toSeq) ++ (for {
      rowIdx <- 0 until dataFields.size
      colIdx <- rowIdx + 1 until dataFields.size
    } yield double(s"corr_${fieldSeq(rowIdx)}_${fieldSeq(colIdx)}"))
    val schema = StructType(targetFields)

    ctx.createDataFrame(targetRDD, schema)
  }

  def toXml: Elem =
    <node method={ method.toString }>
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
    ("tag" -> tag) ~ ("method" -> method.toString) ~ ("groupBy" -> groupBy) ~ ("aggregate" -> aggregate)
  }
}

/**
 * Correlation companion object.
 */
object Correlation {
  val tag = "correlation"

  def apply(dataFields: String*): Correlation = apply(dataFields, Nil)

  def fromXml(xml: Node) = {
    val dataFields = (xml \ "aggregate" \ "field") map { _ \ "@name" asString }
    val groupFields = (xml \ "group-by" \ "field") map (_ \ "@name" asString)
    val method = CorrelationMethod.withName(xml \ "@method" asString)
    apply(dataFields, groupFields, method)
  }

  def fromJson(json: JValue) = {
    val dataFields = (json \ "aggregate" asArray) map (_ asString)
    val groupFields = (json \ "groupBy" asArray) map (_ asString)
    val method = CorrelationMethod.withName(json \ "method" asString)
    apply(dataFields, groupFields, method)
  }
}