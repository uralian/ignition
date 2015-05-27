package org.apache.spark.streaming

import org.apache.spark.util.ManualClock

/**
 * A wrapper around Spark ManualClock.
 *
 * Taken from http://mkuthan.github.io/blog/2015/03/01/spark-unit-testing/
 */
class ClockWrapper(ssc: StreamingContext) {

  def getTimeMillis(): Long = manualClock().getTimeMillis

  def setTime(timeToSet: Long) = manualClock().setTime(timeToSet)

  def advance(timeToAdd: Long) = manualClock().advance(timeToAdd)

  def waitTillTime(targetTime: Long): Long = manualClock().waitTillTime(targetTime)

  private def manualClock(): ManualClock = {
    ssc.scheduler.clock.asInstanceOf[ManualClock]
  }
}