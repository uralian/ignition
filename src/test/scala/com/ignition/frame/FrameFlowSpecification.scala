package com.ignition.frame

import org.apache.spark.SparkContext
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.types.StructType

import com.ignition.{ ExecutionException, FlowSpecification, SparkTestHelper }

/**
 * Base trait for frame flow spec2 tests, includes some helper functions.
 *
 * @author Vlad Orzhekhovskiy
 */
trait FrameFlowSpecification extends FlowSpecification with SparkTestHelper {
  
  implicit protected val sc: SparkContext = createSparkContext

  implicit protected val ctx: SQLContext = createSQLContxt(sc)
  
  implicit protected val rt = new DefaultSparkRuntime(ctx)
  
  import ctx.implicits._

  System.setProperty(com.ignition.STEPS_SERIALIZABLE, false.toString)

  /**
   * Checks if the output schema for a given port index is identical to the supplied schema.
   */
  protected def assertSchema(schema: StructType, step: FrameStep, index: Int = 0) =
    step.outSchema(index) === schema

  /**
   * Checks if the output is identical to the supplied row set.
   */
  protected def assertOutput(step: FrameStep, index: Int, rows: Row*) =
    assertDataFrame(step.output(index, false), rows: _*)

  /**
   * Checks if the limited output is identical to the supplied row set.
   */
  protected def assertPreview(step: FrameStep, index: Int, rows: Row*) =
    assertDataFrame(step.output(index, true), rows: _*)

  /**
   * Checks if the data frame is identical to the supplied row set.
   */
  protected def assertDataFrame(df: DataFrame, rows: Row*) =
    df.collect.toSet === rows.toSet
    
  /**
   * Closes Spark context and clears the port assignments.
   */
  override def afterAll = clearContext(sc)
}