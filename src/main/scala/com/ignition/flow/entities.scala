package com.ignition.flow

import scala.xml.Elem

import org.apache.spark.sql.DataFrame

import com.ignition._

/**
 * XML serialization.
 */
trait XmlExport {
  def toXml: Elem
}

/**
 * Workflow step that emits DataFrame as the output.
 */
trait FlowStep extends XStep[DataFrame] {

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

abstract class Producer extends XProducer[DataFrame] with FlowStep

abstract class Transformer extends XTransformer[DataFrame] with FlowStep

abstract class Splitter(outputCount: Int)
  extends XSplitter[DataFrame](outputCount) with FlowStep

abstract class Merger(inputCount: Int)
  extends XMerger[DataFrame](inputCount) with FlowStep

abstract class Module(inputCount: Int, outputCount: Int)
  extends XModule[DataFrame](inputCount, outputCount) with FlowStep