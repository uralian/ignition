package com.ignition.stream

import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.Duration

import com.ignition.SparkRuntime

/**
 * Sliding time window.
 *
 * @author Vlad Orzhekhovskiy
 */

case class Window(windowDuration: Duration, slideDuration: Option[Duration] = None) extends StreamTransformer {
  protected def compute(arg: DataStream, limit: Option[Int])(implicit runtime: SparkRuntime): DataStream =
    arg.window(windowDuration, slideDuration getOrElse arg.slideDuration)

  protected def computeSchema(inSchema: StructType)(implicit runtime: SparkRuntime): StructType = inSchema
  
  def toXml: scala.xml.Elem = ???
  def toJson: org.json4s.JValue = ???
}

/**
 * Sliding window companion object.
 *
 */
object Window {
  def apply(windowDuration: Duration, slideDuration: Duration): Window =
    apply(windowDuration, Some(slideDuration))
}