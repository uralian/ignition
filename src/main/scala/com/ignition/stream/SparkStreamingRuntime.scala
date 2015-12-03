package com.ignition.stream

import org.apache.spark.streaming.StreamingContext
import com.ignition.frame.SparkRuntime
import org.apache.spark.sql.SQLContext
import com.ignition.frame.DefaultSparkRuntime

/**
 * An extension of [[com.ignition.frame.SparkRuntime]] which adds a streaming context to the mix.
 */
trait SparkStreamingRuntime extends SparkRuntime {
  def ssc: StreamingContext
}

/**
 * The default implementation of [[SparkStreamingRuntime]].
 */
class DefaultSparkStreamingRuntime(ctx: SQLContext, @transient val ssc: StreamingContext)
    extends DefaultSparkRuntime(ctx) with SparkStreamingRuntime {
}