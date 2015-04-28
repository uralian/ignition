package com.ignition

import org.apache.spark.sql.types.Decimal

/**
 * Helper methods to generate test data.
 *
 * @author Vlad Orzhekhovskiy
 */
trait TestDataHelper {

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