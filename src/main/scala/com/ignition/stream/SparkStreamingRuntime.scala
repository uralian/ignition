package com.ignition.stream

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{ Duration, StreamingContext, StreamingContextState }

import com.ignition.{ ExecutionException, SparkHelper }
import com.ignition.frame.{ DefaultSparkRuntime, SparkRuntime }

/**
 * An extension of [[com.ignition.frame.SparkRuntime]] which adds a streaming context to the mix.
 */
trait SparkStreamingRuntime extends SparkRuntime {

  /**
   * The current streaming context.
   */
  def ssc: StreamingContext

  /**
   * Returns true if the streaming context is currently active.
   */
  def isRunning: Boolean = ssc != null && ssc.getState == StreamingContextState.ACTIVE

  /**
   * Starts streaming. Any modification to the workflow configuration are not allowed
   * while streaming is on.
   */
  def start(): Unit

  /**
   * Stops streaming.
   */
  def stop(): Unit

  /**
   * Restarts the runtime.
   */
  def restart() = { stop; start }

  /**
   * Registers the step with the runtime. Subsequent restarts will keep binding the
   * step to newly created contexts.
   */
  private[ignition] def register(step: StreamStep): Unit

  /**
   * Unregisters the step with the runtime. After the restart, the step will no longer
   * be bound to the active context.
   */
  private[ignition] def unregister(step: StreamStep): Unit
}

/**
 * The default implementation of [[SparkStreamingRuntime]].
 */
class DefaultSparkStreamingRuntime(ctx: SQLContext, batchDuration: Duration)
    extends DefaultSparkRuntime(ctx) with SparkStreamingRuntime {

  @transient private var steps: Set[StreamStep] = Set.empty

  @transient private var _ssc: StreamingContext = SparkHelper.createStreamingContext(batchDuration)

  def ssc: StreamingContext = _ssc

  private[ignition] def register(step: StreamStep) = synchronized { steps = steps + step }

  private[ignition] def unregister(step: StreamStep) = synchronized { steps = steps - step }

  def start(): Unit = synchronized {
    assert(!isRunning, "Streaming context already active")
    steps foreach bindToSSC
    _ssc.start
    log.info("Streaming Context started")
  }

  def stop(): Unit = synchronized {
    assert(isRunning, "No active streaming context")
    SparkHelper.stop(_ssc)
    _ssc = SparkHelper.createStreamingContext(batchDuration)
    steps foreach (_.resetCache(true, true))
  }

  private def bindToSSC(step: StreamStep) = step.evaluate(this) foreach {
    _.foreachRDD(_ => {})
  }
}