package com.ignition.stream

import com.ignition._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.Row

/**
 * Workflow step that emits DStream[Row] as the output.
 */
trait StreamStep extends Step[DStream[Row]] {

  /**
   * Returns the implicit SparkContext.
   */
  protected def sc(implicit runtime: SparkRuntime) = runtime.sc

  /**
   * Returns the implicit StreamingContext.
   */
  protected def ssc(implicit runtime: SparkRuntime) = runtime.ssc
}

/* step templates */

abstract class StreamProducer extends Producer[DStream[Row]] with StreamStep

abstract class StreamTransformer extends Transformer[DStream[Row]] with StreamStep

abstract class StreamSplitter(outputCount: Int)
  extends Splitter[DStream[Row]](outputCount) with StreamStep

abstract class StreamMerger(inputCount: Int)
  extends Merger[DStream[Row]](inputCount) with StreamStep

abstract class StreamModule(inputCount: Int, outputCount: Int)
  extends Module[DStream[Row]](inputCount, outputCount) with StreamStep