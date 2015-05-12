package com.ignition.flow.mllib

import com.ignition.flow.Transformer
import org.apache.spark.sql.DataFrame
import org.apache.spark.broadcast.Broadcast
import com.ignition.types._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SQLContext

/**
 * An abstract MLLib step, provides helper functions for concrete implementations.
 *
 * @author Vlad Orzhekhovskiy
 */
abstract class AbstractMLStep extends Transformer {

  /**
   * Converts a data frame into a pair RDD[(key, data)], where key is the row key as defined
   * by the set of grouping fields, and data is a data Vector, as defined by fields.
   */
  protected def partitionByKey(df: DataFrame, fields: Iterable[String],
    groupFields: Iterable[String])(implicit ctx: SQLContext) = {

    val indexMap = df.schema.indexMap
    val fieldIndices = fields map indexMap toSeq
    val groupIndices = groupFields map indexMap toSeq

    df mapPartitions { rows =>
      rows map { row =>
        val key = row.subrow(groupIndices: _*)
        val values = row.subrow(fieldIndices: _*).toSeq map (_.asInstanceOf[Number].doubleValue)
        val vector = Vectors.dense(values.toArray)
        (key, vector)
      }
    } partitionBy (new HashPartitioner(ctx.sparkContext.defaultParallelism))
  }
}