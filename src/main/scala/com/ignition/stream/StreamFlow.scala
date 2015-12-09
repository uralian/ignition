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

  /**
   * Starts a stream flow.
   */
  def start(implicit runtime: SparkStreamingRuntime): Unit = {
    outPoints foreach (_.value(false).foreachRDD(_ => {}))

    runtime.ssc.start
    runtime.ssc.awaitTermination
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