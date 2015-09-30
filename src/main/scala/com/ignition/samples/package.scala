package com.ignition

import org.apache.spark.sql.types.Decimal
import com.eaio.uuid.UUID

/**
 * Helper functions for Ignition samples.
 *
 * @author Vlad Orzhekhovskiy
 */
package object samples {

  /**
   * Creates a new UUID.
   */
  protected[samples] def newid = new UUID().toString

  /**
   * Constructs a java.sql.Date instance.
   */
  protected[samples] def javaDate(year: Int, month: Int, day: Int) = java.sql.Date.valueOf(s"$year-$month-$day")

  /**
   * Constructs a java.sql.Timestamp instance.
   */
  protected[samples] def javaTime(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int = 0) =
    java.sql.Timestamp.valueOf(s"$year-$month-$day $hour:$minute:$second")

  /**
   * Constructs a java.math.BigDecimal instance.
   */
  protected[samples] def javaBD(x: Double) = Decimal(x).toJavaBigDecimal

  /**
   * Constructs a java.math.BigDecimal instance.
   */
  protected[samples] def javaBD(str: String) = Decimal(str).toJavaBigDecimal
}