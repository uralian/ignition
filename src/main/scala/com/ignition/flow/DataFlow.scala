package com.ignition.flow

import org.apache.spark.sql.SQLContext

/**
 * Data Flow represents an executable job.
 *
 * @author Vlad Orzhekhovskiy
 */
case class DataFlow(targets: Iterable[Step]) {

  /**
   * Executes a data flow.
   */
  def execute(implicit ctx: SQLContext): Unit = (for {
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
    case step: Step => new DataFlow(Seq(step))
    case _ => new DataFlow(steps.productIterator.asInstanceOf[Iterator[Step]].toSeq)
  }
}