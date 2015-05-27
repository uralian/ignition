package com.ignition.frame

import com.ignition.SparkRuntime
import org.apache.spark.sql.DataFrame

/**
 * Data Flow represents an executable job.
 *
 * @author Vlad Orzhekhovskiy
 */
case class DataFlow(targets: Iterable[FrameStep]) {

  /**
   * Executes a data flow.
   */
  def execute(implicit runtime: SparkRuntime): Unit = (for {
    tgt <- targets
    index <- 0 until tgt.outputCount
  } yield (tgt, index)) foreach {
    case (tgt, index) => tgt.output(index)
  }
}

/**
 * DataFlow companion object.
 */
object DataFlow {
  def apply(steps: Product): DataFlow = steps match {
    case step: FrameStep => new DataFlow(Seq(step))
    case _ => new DataFlow(steps.productIterator.asInstanceOf[Iterator[FrameStep]].toSeq)
  }
}