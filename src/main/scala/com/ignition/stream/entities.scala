package com.ignition.stream

import com.ignition._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.Row
import org.apache.spark.streaming.StreamingContext

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