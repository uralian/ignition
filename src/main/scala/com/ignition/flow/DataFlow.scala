package com.ignition.flow

import com.ignition.SparkRuntime
import org.apache.spark.sql.DataFrame

/**
 * Data Flow represents an executable job.
 *
 * @author Vlad Orzhekhovskiy
 */
case class DataFlow(targets: Iterable[FlowStep]) {

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
    case step: FlowStep => new DataFlow(Seq(step))
    case _ => new DataFlow(steps.productIterator.asInstanceOf[Iterator[FlowStep]].toSeq)
  }
}