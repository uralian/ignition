package com.ignition.stream

import scala.xml.Node

import org.json4s.JValue

import com.ignition.{ ConnectionSource, Step, SubModule, outs }

/**
 * Stream Flow represents a DStream workflow.
 *
 * @author Vlad Orzhekhovskiy
 */
case class StreamFlow(targets: Iterable[ConnectionSource[DataStream, SparkStreamingRuntime]])
    extends SubModule[DataStream, SparkStreamingRuntime]((Nil, targets.toSeq)) with StreamStep {

  val tag = StreamFlow.tag

  @transient private var flowListeners = Set.empty[StreamFlowListener]

  /**
   * Registers a flow listener.
   */
  def addStreamFlowListener(listener: StreamFlowListener) = flowListeners += listener

  /**
   * Unregisters a flow listener.
   */
  def removeStreamFlowListener(listener: StreamFlowListener) = flowListeners -= listener

  /**
   * Starts a stream flow.
   */
  def start(implicit runtime: SparkStreamingRuntime): Unit = {
    outPoints foreach (_.value(false).foreachRDD(_ => {}))

    runtime.ssc.start
    notifyListeners(new StreamFlowStarted(this))

    runtime.ssc.awaitTermination
    notifyListeners(new StreamFlowTerminated(this))
  }

  /**
   * Notifies all listeners.
   */
  private def notifyListeners(event: StreamFlowEvent) = event match {
    case e: StreamFlowStarted    => flowListeners foreach (_.onStreamFlowStarted(e))
    case e: StreamFlowTerminated => flowListeners foreach (_.onStreamFlowTerminated(e))
  }
}

/**
 * StreamFlow companion object.
 */
object StreamFlow {
  val tag = "streamflow"

  private type DSS = Step[DataStream, SparkStreamingRuntime]

  def apply(step: DSS): StreamFlow = (allOuts _ andThen apply)(Tuple1(step))

  def apply(tuple: Product2[DSS, DSS]): StreamFlow = (allOuts _ andThen apply)(tuple)

  def apply(tuple: Product3[DSS, DSS, DSS]): StreamFlow = (allOuts _ andThen apply)(tuple)

  def apply(tuple: Product4[DSS, DSS, DSS, DSS]): StreamFlow = (allOuts _ andThen apply)(tuple)

  def apply(tuple: Product5[DSS, DSS, DSS, DSS, DSS]): StreamFlow = (allOuts _ andThen apply)(tuple)

  /**
   * Collects the outgoing ports, assuming the argument contains only Step[DataStream] instances.
   * It is safe since it is only called from the internal methods of this class.
   */
  private def allOuts(tuple: Product) = tuple.productIterator.toList flatMap { x =>
    val step = x.asInstanceOf[DSS]
    (0 until step.outputCount) map outs(step)
  }

  def fromXml(xml: Node) = {
    val subflow = StreamSubFlow.fromXml(xml)
    StreamFlow(subflow.outPoints)
  }

  def fromJson(json: JValue) = {
    val subflow = StreamSubFlow.fromJson(json)
    StreamFlow(subflow.outPoints)
  }
}

/**
 * Base trait for all stream flow events.
 */
sealed trait StreamFlowEvent {
  def flow: StreamFlow
}

case class StreamFlowStarted(flow: StreamFlow) extends StreamFlowEvent

case class StreamFlowTerminated(flow: StreamFlow) extends StreamFlowEvent

/**
 * Listener which will be notified on stream flow events.
 */
trait StreamFlowListener {
  /**
   * Called when the stream flow has been started.
   */
  def onStreamFlowStarted(event: StreamFlowStarted)

  /**
   * Called when the stream flow has been terminated.
   */
  def onStreamFlowTerminated(event: StreamFlowTerminated)
}