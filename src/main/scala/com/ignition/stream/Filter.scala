package com.ignition.stream

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.DStream

import com.ignition.SparkRuntime
import com.ignition.frame.RowCondition

/**
 * Filters the stream based on a combination of boolean conditions against fields.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Filter(condition: RowCondition) extends StreamSplitter(2) {
  def and(c: RowCondition) = copy(condition = this.condition.and(c))
  def or(c: RowCondition) = copy(condition = this.condition.or(c))

  protected def compute(arg: DataStream, index: Int, limit: Option[Int])(implicit runtime: SparkRuntime): DataStream = {
    val func = if (index == 0) condition else !condition
    arg filter (func(_))
  }

  protected def computeSchema(inSchema: StructType, index: Int)(implicit runtime: SparkRuntime): StructType = inSchema

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}