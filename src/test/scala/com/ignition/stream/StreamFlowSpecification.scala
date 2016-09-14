package com.ignition.stream

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.control.NonFatal

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.scheduler._

import com.ignition.frame.FrameFlowSpecification

/**
 * Base trait for stream flow spec2 tests, includes some helper functions.
 *
 * @author Vlad Orzhekhovskiy
 */
trait StreamFlowSpecification extends FrameFlowSpecification {

  System.setProperty(com.ignition.STEPS_SERIALIZABLE, true.toString)

  val batchDuration = Milliseconds(100)

  /**
   * Starts the streaming, waits for a certain number of batches (using a fake clock)
   * and compares the stream output with the provided result.
   */
  protected def runAndAssertOutput(step: StreamStep, index: Int, batchCount: Int, expected: Set[Row]*) = {
    step.resetCache(true, true)

    implicit val rt = new DefaultSparkStreamingRuntime(ctx, batchDuration)

    var buffer = ListBuffer.empty[Set[Row]]
    step.register

    val listen = new StreamingListener {
      private var batchIndex = 0
      override def onBatchCompleted(event: StreamingListenerBatchCompleted) = synchronized {
        batchIndex += 1
        if (batchIndex == batchCount) Future { rt.stop }
      }
      override def onReceiverError(error: StreamingListenerReceiverError) = synchronized {
        log.error("Receiver error occurred: " + error)
        Future { rt.stop }
      }
    }
    rt.ssc.addStreamingListener(listen)

    try {
      rt.start
      rt.ssc.awaitTermination
    } catch {
      case NonFatal(e) =>
        log.error("Error occurred while streaming: " + e.getMessage)
        rt.stop
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