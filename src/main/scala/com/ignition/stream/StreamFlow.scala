package com.ignition.stream

import com.ignition.{ ExecutionException, SparkRuntime }

/**
 * Stream Flow represents a DStream workflow.
 *
 * @author Vlad Orzhekhovskiy
 */
case class StreamFlow(targets: Iterable[StreamStep]) {

  /**
   * Starts a stream flow.
   */
  def start(implicit runtime: SparkRuntime): Unit = {
    val ports = for {
      tgt <- targets
      index <- 0 until tgt.outputCount
    } yield (tgt, index)

//    ports foreach {
//      case (tgt, index) => tgt.output(index).foreachRDD(_ => {})
//    }

    runtime.ssc.start
    runtime.ssc.awaitTermination
  }
}

/**
 * StreamFlow companion object.
 */
object StreamFlow {
  def apply(steps: Product): StreamFlow = steps match {
    case step: StreamStep => new StreamFlow(Seq(step))
    case _ => new StreamFlow(steps.productIterator.asInstanceOf[Iterator[StreamStep]].toSeq)
  }
}