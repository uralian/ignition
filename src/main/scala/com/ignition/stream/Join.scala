package com.ignition.stream

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Column, Row }
import org.apache.spark.sql.types.StructType

import com.ignition.SparkRuntime
import com.ignition.frame.JoinType.{ INNER, JoinType }

/**
 * Performs join of the two data streams.
 * In row conditions, if there is ambiguity in a field's name, use "input0" and "input1"
 * prefixes for the first and second input respectively.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Join(condition: Option[Column], joinType: JoinType) extends StreamMerger(2) {

  def joinType(jt: JoinType) = copy(joinType = jt)

  protected def compute(args: Seq[DataStream], limit: Option[Int])(implicit runtime: SparkRuntime): DataStream = {
    val stream1 = args(0)
    val stream2 = args(1)

    stream1.transformWith(stream2, (rdd1: RDD[Row], rdd2: RDD[Row]) => {
      if (rdd1.isEmpty) rdd2
      else if (rdd2.isEmpty) rdd1
      else {
        val df1 = ctx.createDataFrame(rdd1, rdd1.first.schema).as('input0)
        val df2 = ctx.createDataFrame(rdd2, rdd2.first.schema).as('input1)
        val df = condition map (c => df1.join(df2, c, joinType.toString)) getOrElse df1.join(df2)
        df.rdd
      }
    })
  }

  protected def computeSchema(inSchemas: Seq[StructType])(implicit runtime: SparkRuntime): StructType =
    computedSchema(0)

  def toXml: scala.xml.Elem = ???
  def toJson: org.json4s.JValue = ???
}

/**
 * Join companion object.
 */
object Join {

  def apply(): Join = apply(None, INNER)

  def apply(condition: Column): Join = apply(condition, INNER)

  def apply(condition: Column, joinType: JoinType): Join = apply(Some(condition), joinType)

  def apply(condition: String): Join = apply(condition, INNER)

  def apply(condition: String, joinType: JoinType): Join = apply(new Column(condition), joinType)
}