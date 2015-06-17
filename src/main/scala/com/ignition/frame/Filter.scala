package com.ignition.frame

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import com.ignition.SparkRuntime

/**
 * Filters the data frame based on a combination of boolean conditions against fields.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Filter(condition: RowCondition) extends FrameSplitter(2) {
  def and(c: RowCondition) = copy(condition = this.condition.and(c))
  def or(c: RowCondition) = copy(condition = this.condition.or(c))

  protected def compute(arg: DataFrame, index: Int, limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
    val df = optLimit(arg, limit)
    val func = if (index == 0) condition else !condition
    val rdd = df.rdd filter (func(_))
    ctx.createDataFrame(rdd, arg.schema)
  }

  protected def computeSchema(inSchema: StructType, index: Int)(implicit runtime: SparkRuntime): StructType = inSchema
}