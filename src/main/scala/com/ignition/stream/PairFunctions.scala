package com.ignition.stream

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.DStream

import com.ignition.SparkRuntime
import com.ignition.types.{ RichRow, RichStructType }

/**
 * Supplies helper functions for pair streams.
 *
 * @author Vlad Orzhekhovskiy
 */
trait PairFunctions { self: StreamStep =>

  /**
   * Converts a data frame into a pair DStream[(key, data)], where key is the row key as defined
   * by the set of grouping fields, and data is defied by the set of data fields from the
   * original row.
   */
  def toPair(stream: DataStream, dataFields: Iterable[String], groupFields: Iterable[String])(implicit runtime: SparkRuntime): DStream[(Row, Row)] = stream transform { rdd =>
    if (rdd.isEmpty)
      rdd.sparkContext.emptyRDD
    else if (dataFields.isEmpty) {
      val indexMap = rdd.first.schema.indexMap
      val groupIndices = groupFields map indexMap toSeq

      partitionByKey(rdd, groupIndices, identity)
    } else {
      val indexMap = rdd.first.schema.indexMap
      val dataIndices = dataFields map indexMap toSeq
      val groupIndices = groupFields map indexMap toSeq

      partitionByKey(rdd, groupIndices, _.subrow(dataIndices: _*))
    }
  }

  /**
   * Converts the data frame into an RDD[(key, value)] where key is defined by the group
   * field indices, and value is computed for each row by the supplied function.
   */
  def partitionByKey[T: ClassTag](rdd: RDD[Row], groupIndices: Seq[Int],
                                  func: Row => T)(implicit runtime: SparkRuntime): RDD[(Row, T)] = rdd mapPartitions { rows =>
    rows map { row =>
      val key = row.subrow(groupIndices: _*)
      val value = func(row)
      (key, value)
    }
  }
}