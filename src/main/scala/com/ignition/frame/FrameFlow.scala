package com.ignition.frame

import scala.xml.Node

import org.apache.spark.sql.DataFrame
import org.json4s.JValue

import com.ignition.{ ConnectionSource, Step, SubModule, outs }

/**
 * Frame Flow represents an executable job.
 *
 * @author Vlad Orzhekhovskiy
 */
case class FrameFlow(targets: Iterable[ConnectionSource[DataFrame, SparkRuntime]])
    extends SubModule[DataFrame, SparkRuntime]((Nil, targets.toSeq)) with FrameStep {

  val tag = FrameFlow.tag

  @transient private var flowListeners = Set.empty[FrameFlowListener]

  /**
   * Registers a flow listener.
   */
  def addFrameFlowListener(listener: FrameFlowListener) = flowListeners += listener

  /**
   * Unregisters a flow listener.
   */
  def removeFrameFlowListener(listener: FrameFlowListener) = flowListeners -= listener

  /**
   * Executes a frame flow.
   */
  def execute(preview: Boolean)(implicit runtime: SparkRuntime): Iterable[DataFrame] = {
    notifyListeners(new FrameFlowStarted(this))
    val results = outPoints map (_.value)
    notifyListeners(new FrameFlowComplete(this, results))
    results
  }

  /**
   * Executes a frame flow.
   */
  def execute(implicit runtime: SparkRuntime): Iterable[DataFrame] = execute(false)

  /**
   * Notifies all listeners.
   */
  private def notifyListeners(event: FrameFlowEvent) = event match {
    case e: FrameFlowStarted  => flowListeners foreach (_.onFrameFlowStarted(e))
    case e: FrameFlowComplete => flowListeners foreach (_.onFrameFlowComplete(e))
  }
}

/**
 * FrameFlow companion object.
 */
object FrameFlow {
  val tag = "frameflow"

  private type DFS = Step[DataFrame, SparkRuntime]

  def apply(step: DFS): FrameFlow = (allOuts _ andThen apply)(Tuple1(step))

  def apply(tuple: Product2[DFS, DFS]): FrameFlow = (allOuts _ andThen apply)(tuple)

  def apply(tuple: Product3[DFS, DFS, DFS]): FrameFlow = (allOuts _ andThen apply)(tuple)

  def apply(tuple: Product4[DFS, DFS, DFS, DFS]): FrameFlow = (allOuts _ andThen apply)(tuple)

  def apply(tuple: Product5[DFS, DFS, DFS, DFS, DFS]): FrameFlow = (allOuts _ andThen apply)(tuple)

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
    FrameFlow(subflow.outPoints)
  }

  def fromJson(json: JValue) = {
    val subflow = FrameSubFlow.fromJson(json)
    FrameFlow(subflow.outPoints)
  }
}

/**
 * Base trait for all frame flow events.
 */
sealed trait FrameFlowEvent {
  def flow: FrameFlow
}

case class FrameFlowStarted(flow: FrameFlow) extends FrameFlowEvent

case class FrameFlowComplete(flow: FrameFlow, results: Seq[DataFrame]) extends FrameFlowEvent

/**
 * Listener which will be notified on frame flow events.
 */
trait FrameFlowListener {
  /**
   * Called when the frame flow has been started.
   */
  def onFrameFlowStarted(event: FrameFlowStarted)

  /**
   * Called when the frame flow has been complete.
   */
  def onFrameFlowComplete(event: FrameFlowComplete)
}