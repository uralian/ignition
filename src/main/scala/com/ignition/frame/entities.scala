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

  /**
   * Calls output() passing the limit of 1 and returns the DataFrame schema.
   * This method should be used by subclasses as an easy way to return the schema.
   */
  protected def computedSchema(index: Int)(implicit runtime: SparkRuntime) =
    output(index, Some(1)).schema
}

/* step templates */

abstract class FrameProducer extends Producer[DataFrame] with FrameStep

abstract class FrameTransformer extends Transformer[DataFrame] with FrameStep

abstract class FrameSplitter(override val outputCount: Int)
  extends Splitter[DataFrame](outputCount) with FrameStep

abstract class FrameMerger(override val inputCount: Int)
  extends Merger[DataFrame](inputCount) with FrameStep

abstract class FrameModule(override val inputCount: Int, override val outputCount: Int)
  extends Module[DataFrame](inputCount, outputCount) with FrameStep