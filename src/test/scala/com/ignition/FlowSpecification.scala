package com.ignition

import java.io.{ ByteArrayOutputStream, IOException, ObjectOutputStream }

import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.specs2.matcher.XmlMatchers
import org.specs2.mutable.Specification

/**
 * Base spec2 trait for workflow testing.
 *
 * @author Vlad Orzhekhovskiy
 */
trait FlowSpecification extends Specification
    with XmlMatchers with TestDataHelper with Logging {

  /**
   * Tests the argument for being unserializable.
   */
  protected def assertUnserializable(obj: Any) = {
    val oos = new ObjectOutputStream(new ByteArrayOutputStream())
    oos.writeObject(obj) must throwA[IOException]
  }

  protected implicit def anySeqToRow(data: Seq[Any]) = Row.fromSeq(data)

  protected implicit def tupleToRow(tuple: Product) = Row.fromTuple(tuple)

  protected val jNone = org.json4s.JNothing
}