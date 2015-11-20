package com.ignition.frame

import scala.reflect.ClassTag

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.{ DataFrame, Row }

import com.ignition.types.{ RichRow, RichStructType }

/**
 * Supplies helper functions for pair RDDs.
 *
 * @author Vlad Orzhekhovskiy
 */
trait PairFunctions { self: FrameStep =>

  /**
   * Converts a data frame into a pair RDD[(key, data)], where key is the row key as defined
   * by the set of grouping fields, and data is defied by the set of data fields from the
   * original row.
   */
  def toPair(df: DataFrame, dataFields: Iterable[String], groupFields: Iterable[String])(implicit runtime: SparkRuntime): RDD[(Row, Row)] = {
    val indexMap = df.schema.indexMap
    val dataIndices = dataFields map indexMap toSeq
    val groupIndices = groupFields map indexMap toSeq

    partitionByKey(df, groupIndices, _.subrow(dataIndices: _*))
  }

  /**
   * Converts the data frame into an RDD[(key, value)] where key is defined by the group
   * field indices, and value is computed for each row by the supplied function.
   */
  def partitionByKey[T: ClassTag](df: DataFrame, groupIndices: Seq[Int],
    func: Row => T)(implicit runtime: SparkRuntime): RDD[(Row, T)] = df mapPartitions { rows =>
    rows map { row =>
      val key = row.subrow(groupIndices: _*)
      val value = func(row)
      (key, value)
    }
  } partitionBy (new HashPartitioner(ctx.sparkContext.defaultParallelism))
}