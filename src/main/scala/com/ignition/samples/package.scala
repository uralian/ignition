package com.ignition

import org.apache.spark.sql.types.Decimal
import com.ignition.frame.DataFlow

/**
 * Helper functions for Ignition samples.
 *
 * @author Vlad Orzhekhovskiy
 */
package object samples {

  /**
   * Creates a new UUID.
   */
  protected[samples] def newid = java.util.UUID.randomUUID.toString 

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

  /**
   * Prints out the XML representation of the data flow.
   */
  protected[samples] def printXml(flow: DataFlow) =
    println(new scala.xml.PrettyPrinter(80, 2).format(flow.toXml))

  /**
   * Prints out the JSON representation of the data flow.
   */
  protected[samples] def printJson(flow: DataFlow) =
    println(org.json4s.jackson.JsonMethods.pretty(flow.toJson))
}