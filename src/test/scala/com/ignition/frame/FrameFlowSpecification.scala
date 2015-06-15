package com.ignition.frame

import org.apache.spark.sql.{ DataFrame, Row }

import com.ignition.{ ExecutionException, FlowSpecification }

/**
 * Base trait for frame flow spec2 tests, includes some helper functions.
 *
 * @author Vlad Orzhekhovskiy
 */
trait FrameFlowSpecification extends FlowSpecification {
  import ctx.implicits._
  
  System.setProperty(com.ignition.STEPS_SERIALIZABLE, false.toString)

  /**
   * Checks if the output is identical to the supplied row set.
   */
  protected def assertOutput(step: FrameStep, index: Int, rows: Row*) =
    assertDataFrame(step.output(index), rows: _*)

  /**
   * Checks if the limited output is identical to the supplied row set.
   */
  protected def assertPreview(step: FrameStep, index: Int, limit: Int, rows: Row*) =
    assertDataFrame(step.output(index, Some(limit)), rows: _*)

  /**
   * Checks if the data frame is identical to the supplied row set.
   */
  protected def assertDataFrame(df: DataFrame, rows: Row*) =
    df.collect.toSet === rows.toSet
}