package com.ignition.frame

import org.apache.spark.sql.{ Column, DataFrame, SQLContext }
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import org.json4s._
import org.json4s.JsonDSL._

import scala.xml.{ Elem, Node }
import scala.xml.NodeSeq.seqToNodeSeq

import com.ignition.SparkRuntime
import com.ignition.util.XmlUtils.RichNodeSeq
import com.ignition.util.JsonUtils.RichJValue

/**
 * Basic aggregate functions.
 */
object BasicAggregator extends Enumeration {

  class BasicAggregator(func: Column => Column, nameFunc: String => String) extends super.Val {
    def createColumn(df: DataFrame, field: String) = (df.col _ andThen func)(field).as(nameFunc(field))
    def apply(field: String) = field -> this
  }
  implicit def valueToAggregator(v: Value) = v.asInstanceOf[BasicAggregator]

  val AVG = new BasicAggregator(avg, _ + "_avg")
  val MIN = new BasicAggregator(min, _ + "_min")
  val MAX = new BasicAggregator(max, _ + "_max")
  val SUM = new BasicAggregator(sum, _ + "_sum")
  val COUNT = new BasicAggregator(count, _ + "_cnt")
  val FIRST = new BasicAggregator(first, _ + "_first")
  val LAST = new BasicAggregator(last, _ + "_last")
  val SUM_DISTINCT = new BasicAggregator(sumDistinct, _ + "_sum_dist")
  val COUNT_DISTINCT = new BasicAggregator(countDistinct(_), _ + "_cnt_dist")
}
import BasicAggregator._

/**
 * Calculates basic statistics about the specified fields.
 *
 * @author Vlad Orzhekhovskiy
 */
case class BasicStats(dataFields: Iterable[(String, BasicAggregator)],
                      groupFields: Iterable[String] = Nil) extends FrameTransformer {

  import BasicStats._

  def add(tuple: (String, BasicAggregator)) = copy(dataFields = dataFields.toSeq :+ tuple)
  def %(tuple: (String, BasicAggregator)) = add(tuple)

  def add(field: String, functions: BasicAggregator*) = copy(functions.map(f => (field, f)))
  def %(field: String, functions: BasicAggregator*) = add(field, functions: _*)

  def groupBy(fields: String*) = copy(groupFields = fields)

  protected def compute(arg: DataFrame, preview: Boolean)(implicit runtime: SparkRuntime): DataFrame = {
    val df = optLimit(arg, preview)

    val groupColumns = groupFields map df.col toSeq
    val aggrColumns = dataFields map {
      case (name, func) => func.createColumn(df, name)
    }
    val columns = groupColumns ++ aggrColumns
    df.groupBy(groupColumns: _*).agg(columns.head, columns.tail: _*)
  }

  def toXml: Elem =
    <node>
      <aggregate>
        {
          dataFields map { case (name, func) => <field name={ name } type={ func.toString }/> }
        }
      </aggregate>
      {
        if (!groupFields.isEmpty)
          <group-by>
            { groupFields map (f => <field name={ f }/>) }
          </group-by>
      }
    </node>.copy(label = tag)

  def toJson: JValue = {
    val groupBy = if (groupFields.isEmpty) None else Some(groupFields)
    val aggregate = dataFields map (df => df._1 -> df._2.toString)
    ("tag" -> tag) ~ ("groupBy" -> groupBy) ~ ("aggregate" -> aggregate)
  }
}

/**
 * Basic Stats companion object.
 */
object BasicStats {
  val tag = "basic-stats"

  def apply(dataFields: (String, BasicAggregator)*): BasicStats = apply(dataFields)

  def fromXml(xml: Node) = {
    val dataFields = (xml \ "aggregate" \ "field") map { node =>
      val name = node \ "@name" asString
      val func = BasicAggregator.withName(node \ "@type" asString): BasicAggregator
      name -> func
    }
    val groupFields = (xml \ "group-by" \ "field") map (_ \ "@name" asString)
    apply(dataFields, groupFields)
  }

  def fromJson(json: JValue) = {
    val dataFields = for {
      JObject(df) <- json \ "aggregate"
      JField(name, JString(funcName)) <- df
      func = BasicAggregator.withName(funcName): BasicAggregator
    } yield name -> func
    val groupFields = (json \ "groupBy" asArray) map (_ asString)
    apply(dataFields, groupFields)
  }
}