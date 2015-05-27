package com.ignition.stream

import scala.collection.mutable.ListBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.streaming.ClockWrapper

import com.ignition.{ ExecutionException, FlowSpecification }

/**
 * Base trait for stream flow spec2 tests, includes some helper functions.
 *
 * @author Vlad Orzhekhovskiy
 */
trait StreamFlowSpecification extends FlowSpecification {

  /**
   * Starts the streaming, waits for a certain number of batches (using a fake clock)
   * and compares the stream output with the provided result.
   */
  protected def runAndAssertOutput(step: StreamStep, index: Int, batchCount: Int, result: Set[Row]*) = {
    var buffer = ListBuffer.empty[RDD[Row]]
    step.output(index).foreachRDD(rdd => buffer += rdd)

    val clock = new ClockWrapper(ssc)
    ssc.start
    clock.advance(batchCount * batchDuration.getMillis)

    Thread.sleep(batchCount * 100)

    (buffer zip result) forall {
      case (rdd, rows) => rdd.collect.toSet === rows
    }

    ssc.stop(false, false)
    ssc.awaitTerminationOrTimeout(100)
  }

  /**
   * Checks if the data frame is identical to the supplied row set.
   */
  protected def assertRDD(rdd: RDD[Row], rows: Row*) =
    rdd.collect.toSet === rows.toSet
}