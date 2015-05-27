package com.ignition.frame

import java.io.{ ByteArrayOutputStream, IOException, ObjectOutputStream }

import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types.StructType
import org.specs2.matcher.XmlMatchers
import org.specs2.mutable.Specification

import com.ignition.{ ExecutionException, SparkTestHelper, TestDataHelper, Step }

/**
 * Base trait for frame flow spec2 tests, includes some helper functions.
 *
 * @author Vlad Orzhekhovskiy
 */
trait FrameFlowSpecification extends Specification with XmlMatchers with SparkTestHelper with TestDataHelper {
  import ctx.implicits._

  /**
   * Tests the argument for being unserializable.
   */
  protected def assertUnserializable(obj: Any) = {
    val oos = new ObjectOutputStream(new ByteArrayOutputStream())
    oos.writeObject(obj) must throwA[IOException]
  }

  /**
   * Checks if the output schema for a given port index is identical to the supplied schema.
   */
  protected def assertSchema(schema: StructType, step: Step[_], index: Int = 0) =
    step.outSchema(index) === schema

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

  protected implicit def anySeqToRow(data: Seq[Any]) = Row.fromSeq(data)

  protected implicit def tupleToRow(tuple: Product) = Row.fromTuple(tuple)
}