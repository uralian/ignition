package com.ignition.flow

import org.apache.spark.sql.DataFrame

import com.ignition._

/**
 * Workflow step that emits DataFrame as the output.
 */
trait FlowStep extends Step[DataFrame] {

  /**
   * Returns the implicit SQLContext.
   */
  protected def ctx(implicit runtime: SparkRuntime) = runtime.ctx

  /**
   * Returns the implicit SparkContext.
   */
  protected def sparkContext(implicit runtime: SparkRuntime) = runtime.sc

  /**
   * Optionally limits the data frame.
   */
  protected def optLimit(df: DataFrame, limit: Option[Int]) = limit map df.limit getOrElse df
}

/* step templates */

abstract class FlowProducer extends Producer[DataFrame] with FlowStep

abstract class FlowTransformer extends Transformer[DataFrame] with FlowStep

abstract class FlowSplitter(outputCount: Int)
  extends Splitter[DataFrame](outputCount) with FlowStep

abstract class FlowMerger(inputCount: Int)
  extends Merger[DataFrame](inputCount) with FlowStep

abstract class FlowModule(inputCount: Int, outputCount: Int)
  extends Module[DataFrame](inputCount, outputCount) with FlowStep