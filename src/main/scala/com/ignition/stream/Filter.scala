package com.ignition.stream

import org.apache.spark.sql.{ Column, DataFrame }
import org.apache.spark.sql.types.StructType

import com.ignition.SparkRuntime

/**
 * Filters the stream based on a combination of boolean conditions against fields.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Filter(condition: Column) extends StreamSplitter(2) {

  protected def compute(arg: DataStream, index: Int, limit: Option[Int])(implicit runtime: SparkRuntime): DataStream = {
    implicit val sqlContext = ctx

    val column = if (index == 0) condition else !condition
    val filterFunc = (df: DataFrame) => df.filter(column)

    transformAsDF(filterFunc)(arg)
  }

  protected def computeSchema(inSchema: StructType, index: Int)(implicit runtime: SparkRuntime): StructType = inSchema
}