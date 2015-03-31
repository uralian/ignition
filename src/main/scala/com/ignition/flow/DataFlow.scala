package com.ignition.flow

import org.apache.spark.sql.SQLContext
import scala.concurrent._

/**
 * @author Vlad Orzhekhovskiy
 */
case class DataFlow(targets: Iterable[Step]) {

  /**
   * Executes a data flow.
   */
  def execute(implicit ctx: SQLContext): Unit = for {
    tgt <- targets
    index <- 0 until tgt.outputCount
  } yield tgt.output(index)
}

object DataFlow {
  def apply(step: Step): DataFlow = new DataFlow(Seq(step))

  def apply(steps: Tuple2[Step, Step]): DataFlow =
    new DataFlow(steps.productIterator.asInstanceOf[Iterator[Step]].toSeq)
  def apply(steps: Tuple3[Step, Step, Step]): DataFlow =
    new DataFlow(steps.productIterator.asInstanceOf[Iterator[Step]].toSeq)
  def apply(steps: Tuple4[Step, Step, Step, Step]): DataFlow =
    new DataFlow(steps.productIterator.asInstanceOf[Iterator[Step]].toSeq)
  def apply(steps: Tuple5[Step, Step, Step, Step, Step]): DataFlow =
    new DataFlow(steps.productIterator.asInstanceOf[Iterator[Step]].toSeq)
}