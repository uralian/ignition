package com.ignition

import java.io.{ ByteArrayOutputStream, IOException, ObjectOutputStream }
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.specs2.matcher.XmlMatchers
import org.specs2.mutable.Specification
import org.apache.spark.Logging

/**
 * Base spec2 trait for workflow testing.
 *
 * @author Vlad Orzhekhovskiy
 */
trait FlowSpecification extends Specification
  with XmlMatchers
  with SparkTestHelper
  with TestDataHelper with Logging {

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

  protected implicit def anySeqToRow(data: Seq[Any]) = Row.fromSeq(data)

  protected implicit def tupleToRow(tuple: Product) = Row.fromTuple(tuple)

  protected val jNone = org.json4s.JNothing
}