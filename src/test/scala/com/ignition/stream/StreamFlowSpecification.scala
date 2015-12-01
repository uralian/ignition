package com.ignition.stream

import scala.collection.mutable.ListBuffer
import scala.concurrent.future
import scala.util.control.NonFatal

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.streaming.scheduler._

import com.ignition.{ ExecutionException, FlowSpecification }

/**
 * Base trait for stream flow spec2 tests, includes some helper functions.
 *
 * @author Vlad Orzhekhovskiy
 */
trait StreamFlowSpecification extends FlowSpecification {

  System.setProperty(com.ignition.STEPS_SERIALIZABLE, true.toString)

  ssc.stop(true, true)
  System.clearProperty("spark.driver.port")
  System.clearProperty("spark.master.port")

  /**
   * Starts the streaming, waits for a certain number of batches (using a fake clock)
   * and compares the stream output with the provided result.
   */
  protected def runAndAssertOutput(step: StreamStep, index: Int, batchCount: Int, expected: Set[Row]*) = {

    val sc = createSparkContext
    val ctx = createSQLContxt(sc)
    val ssc = createStreamingContext(sc)
    implicit val rt = new DefaultSparkStreamingRuntime(ctx, ssc)

    var buffer = ListBuffer.empty[Set[Row]]
    step.output(index, false).foreachRDD(rdd => buffer += rdd.collect.toSet)

    val listen = new StreamingListener {
      private var batchIndex = 0
      override def onBatchCompleted(event: StreamingListenerBatchCompleted) = synchronized {
        batchIndex += 1
        if (batchIndex >= batchCount) future { ssc.stop(false, true) }
      }
      override def onReceiverError(error: StreamingListenerReceiverError) = synchronized {
        log.error("Receiver error occurred: " + error)
        future { ssc.stop(false, false) }
      }
    }
    ssc.addStreamingListener(listen)

    try {
      ssc.start
      ssc.awaitTermination
    } catch {
      case NonFatal(e) =>
        log.error("Error occurred while streaming: " + e.getMessage)
        ssc.stop(false, false)
    } finally {
      sc.stop
      System.clearProperty("spark.driver.port")
      System.clearProperty("spark.master.port")
    }

    (buffer zip expected) forall {
      case (rdd, rows) => rdd === rows
    }
  }

  /**
   * Checks if the data frame is identical to the supplied row set.
   */
  protected def assertRDD(rdd: RDD[Row], rows: Row*) = rdd.collect.toSet === rows.toSet
}