package com.ignition.flow

import java.io.{ ByteArrayOutputStream, IOException, ObjectOutputStream }
import org.apache.spark.sql.types.{ Decimal, StructType }
import org.specs2.matcher.XmlMatchers
import org.specs2.mutable.Specification
import com.ignition.SparkTestHelper
import org.apache.spark.sql.DataFrame

/**
 * Base trait for flow spec2 tests, includes some helper functions.
 *
 * @author Vlad Orzhekhovskiy
 */
trait FlowSpecification extends Specification with XmlMatchers with SparkTestHelper {
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
  protected def assertSchema(schema: StructType, step: Step, index: Int = 0) =
    step.outputSchema(index) === Some(schema)

  /**
   * Checks if the output is identical to the supplied row set.
   */
  protected def assertOutput(step: Step, index: Int, rows: Seq[Any]*) =
    assertDataFrame(step.output(index), rows: _*)

  /**
   * Checks if the data frame is identical to the supplied row set.
   */
  protected def assertDataFrame(df: DataFrame, rows: Seq[Any]*) =
    df.collect.map(_.toSeq).toSet === Set(rows: _*)

  /**
   * Constructs a java.sql.Date instance.
   */
  protected def javaDate(year: Int, month: Int, day: Int) =
    java.sql.Date.valueOf(s"$year-$month-$day")

  /**
   * Constructs a java.sql.Timestamp instance.
   */
  protected def javaTime(year: Int, month: Int, day: Int, hour: Int, minute: Int) =
    java.sql.Timestamp.valueOf(s"$year-$month-$day $hour:$minute:00")

  /**
   * Constructs a java.math.BigDecimal instance.
   */
  protected def javaBD(x: Double) = Decimal(x).toJavaBigDecimal

  /**
   * Constructs a java.math.BigDecimal instance.
   */
  protected def javaBD(str: String) = Decimal(str).toJavaBigDecimal
}