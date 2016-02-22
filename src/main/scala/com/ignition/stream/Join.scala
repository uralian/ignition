package com.ignition.stream

import scala.xml.{ Elem, Node }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Column, Row }
import org.json4s.JValue
import org.json4s.JsonDSL.{ jobject2assoc, option2jvalue, pair2Assoc, pair2jvalue, string2jvalue }
import org.json4s.jvalue2monadic
import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.RichNodeSeq
import com.ignition.frame.JoinType.{ INNER, JoinType }
import org.apache.spark.sql.catalyst.SqlParser

/**
 * Performs join of the two data streams.
 * In row conditions, if there is ambiguity in a field's name, use "input0" and "input1"
 * prefixes for the first and second input respectively.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Join(condition: Option[String], joinType: JoinType) extends StreamMerger(2) {
  import Join._

  def joinType(jt: JoinType) = copy(joinType = jt)

  protected def compute(args: IndexedSeq[DataStream])(implicit runtime: SparkStreamingRuntime): DataStream = {
    val stream1 = args(0)
    val stream2 = args(1)

    stream1.transformWith(stream2, (rdd1: RDD[Row], rdd2: RDD[Row]) => {
      if (rdd1.isEmpty) rdd2
      else if (rdd2.isEmpty) rdd1
      else {
        val df1 = ctx.createDataFrame(rdd1, rdd1.first.schema).as('input0)
        val df2 = ctx.createDataFrame(rdd2, rdd2.first.schema).as('input1)
        val df = condition map { c =>
          df1.join(df2, new Column(SqlParser.parseExpression(c)), joinType.toString)
        } getOrElse df1.join(df2)
        df.rdd
      }
    })
  }

  def toXml: Elem =
    <node type={ joinType.toString }>
      { condition map (c => <condition>{ c.toString }</condition>) toList }
    </node>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag) ~ ("type" -> joinType.toString) ~
    ("condition" -> condition.map(_.toString))
}

/**
 * Join companion object.
 */
object Join {
  val tag = "stream-join"

  def apply(): Join = apply(None, INNER)

  def apply(condition: Column): Join = apply(condition, INNER)

  def apply(condition: Column, joinType: JoinType): Join = apply(Some(condition.toString), joinType)

  def apply(condition: String): Join = apply(condition, INNER)

  def apply(condition: String, joinType: JoinType): Join = apply(Some(condition), joinType)

  def fromXml(xml: Node) = {
    import com.ignition.frame.JoinType
    val joinType = JoinType.withName(xml \ "@type" asString)
    val condition = (xml \ "condition" getAsString)

    apply(condition, joinType)
  }

  def fromJson(json: JValue) = {
    import com.ignition.frame.JoinType
    val joinType = JoinType.withName(json \ "type" asString)
    val condition = (json \ "condition" getAsString)

    apply(condition, joinType)
  }
}