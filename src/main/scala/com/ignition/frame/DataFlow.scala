package com.ignition.frame

import scala.xml.Node

import org.apache.spark.sql.DataFrame
import org.json4s.JValue

import com.ignition.{ ConnectionSource, Step, SubModule, outs }

/**
 * Data Flow represents an executable job.
 *
 * @author Vlad Orzhekhovskiy
 */
case class DataFlow(targets: Iterable[ConnectionSource[DataFrame, SparkRuntime]])
    extends SubModule[DataFrame, SparkRuntime]((Nil, targets.toSeq)) with FrameStep {

  val tag = DataFlow.tag

  /**
   * Executes a data flow.
   */
  def execute(preview: Boolean)(implicit runtime: SparkRuntime): Iterable[DataFrame] = outPoints map (_.value(preview))

  /**
   * Executes a data flow.
   */
  def execute(implicit runtime: SparkRuntime): Iterable[DataFrame] = execute(false)
}

/**
 * DataFlow companion object.
 */
object DataFlow {
  val tag = "dataflow"

  private type DFS = Step[DataFrame, SparkRuntime]

  def apply(step: DFS): DataFlow = (allOuts _ andThen apply)(Tuple1(step))

  def apply(tuple: Product2[DFS, DFS]): DataFlow = (allOuts _ andThen apply)(tuple)

  def apply(tuple: Product3[DFS, DFS, DFS]): DataFlow = (allOuts _ andThen apply)(tuple)

  def apply(tuple: Product4[DFS, DFS, DFS, DFS]): DataFlow = (allOuts _ andThen apply)(tuple)

  def apply(tuple: Product5[DFS, DFS, DFS, DFS, DFS]): DataFlow = (allOuts _ andThen apply)(tuple)

  /**
   * Collects the outgoing ports, assuming the argument contains only Step[DataFrame] instances.
   * It is safe since it is only called from the internal methods of this class.
   */
  private def allOuts(tuple: Product) = tuple.productIterator.toList flatMap { x =>
    val step = x.asInstanceOf[DFS]
    (0 until step.outputCount) map outs(step)
  }

  def fromXml(xml: Node) = {
    val subflow = FrameSubFlow.fromXml(xml)
    DataFlow(subflow.outPoints)
  }

  def fromJson(json: JValue) = {
    val subflow = FrameSubFlow.fromJson(json)
    DataFlow(subflow.outPoints)
  }
}