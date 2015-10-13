package com.ignition.stream

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
}

/* step templates */

abstract class StreamProducer extends Producer[DataStream] with StreamStep

abstract class StreamTransformer extends Transformer[DataStream] with StreamStep

abstract class StreamSplitter(val outputCount: Int)
  extends Splitter[DataStream] with StreamStep

abstract class StreamMerger(val inputCount: Int)
  extends Merger[DataStream] with StreamStep

abstract class StreamModule(val inputCount: Int, val outputCount: Int)
  extends Module[DataStream] with StreamStep