package com.ignition

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.streaming.dstream.DStream

/**
 * Data types, implicits, aliases for DStream-based workflows.
 *
 * @author Vlad Orzhekhovskiy
 */
package object stream {
  type DataStream = DStream[Row]

  def foreach(tx: Transformer[DataFrame]): Foreach = Foreach(tx)

  /**
   * Converts this RDD into a DataFrame using the schema of the first row, then applies the
   * DataFrame transformation function and returns the resulting RDD.
   */
  def asDF(func: DataFrame => DataFrame)(rdd: RDD[Row])(implicit ctx: SQLContext) = {
    if (rdd.isEmpty) rdd
    else {
      val schema = rdd.first.schema
      val df = ctx.createDataFrame(rdd, schema)
      func(df).rdd
    }
  }

  /**
   * Transforms this data stream using asDF function for each RDD.
   */
  def transformAsDF(func: DataFrame => DataFrame)(stream: DataStream)(implicit ctx: SQLContext) = {
    stream transform (rdd => asDF(func)(rdd))
  }

  /* state functions types */
  type MapState = java.util.Map[String, Any]
  type MapListState = java.lang.Iterable[java.util.Map[String, Any]]
}