package com.ignition.frame

import org.apache.spark.sql.DataFrame

import com.ignition._

/**
 * Workflow step that emits DataFrame as the output.
 */
trait FrameStep extends Step[DataFrame] {

  /**
   * Returns the implicit SQLContext.
   */
  protected def ctx(implicit runtime: SparkRuntime) = runtime.ctx

  /**
   * Returns the implicit SparkContext.
   */
  protected def sc(implicit runtime: SparkRuntime) = runtime.sc

  /**
   * Optionally limits the data frame.
   */
  protected def optLimit(df: DataFrame, limit: Option[Int]) = limit map df.limit getOrElse df
}

/* step templates */

abstract class FrameProducer extends Producer[DataFrame] with FrameStep

abstract class FrameTransformer extends Transformer[DataFrame] with FrameStep

abstract class FrameSplitter(outputCount: Int)
  extends Splitter[DataFrame](outputCount) with FrameStep

abstract class FrameMerger(inputCount: Int)
  extends Merger[DataFrame](inputCount) with FrameStep

abstract class FrameModule(inputCount: Int, outputCount: Int)
  extends Module[DataFrame](inputCount, outputCount) with FrameStep