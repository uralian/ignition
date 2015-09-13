package com.ignition.stream

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }

import com.ignition.{ Merger, Module, Producer, SparkRuntime, Splitter, Step, Transformer }

/**
 * Workflow step that emits DataStream as the output.
 */
trait StreamStep extends Step[DataStream] {

  /**
   * Returns the implicit SQLContext.
   */
  protected def ctx(implicit runtime: SparkRuntime) = runtime.ctx

  /**
   * Returns the implicit SparkContext.
   */
  protected def sc(implicit runtime: SparkRuntime) = runtime.sc

  /**
   * Returns the implicit StreamingContext.
   */
  protected def ssc(implicit runtime: SparkRuntime) = runtime.ssc

  /**
   * The automatically computed schema is not available for streams.
   */
  protected def computedSchema(index: Int)(implicit runtime: SparkRuntime) = {
    ???
  }

  /**
   * Converts this RDD into a DataFrame using the schema of the first row, then applies the
   * DataFrame transformation function and returns the resulting RDD.
   */
  protected def asDF(func: DataFrame => DataFrame)(rdd: RDD[Row])(implicit ctx: SQLContext) = {
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
  protected def transformAsDF(func: DataFrame => DataFrame)(stream: DataStream)(implicit ctx: SQLContext) = {
    stream transform (rdd => asDF(func)(rdd))
  }
}

/* step templates */

abstract class StreamProducer extends Producer[DataStream] with StreamStep

abstract class StreamTransformer extends Transformer[DataStream] with StreamStep

abstract class StreamSplitter(outputCount: Int)
  extends Splitter[DataStream](outputCount) with StreamStep

abstract class StreamMerger(inputCount: Int)
  extends Merger[DataStream](inputCount) with StreamStep

abstract class StreamModule(inputCount: Int, outputCount: Int)
  extends Module[DataStream](inputCount, outputCount) with StreamStep