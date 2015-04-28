package com.ignition.script

import org.joda.time._

/**
 * Functions supported for script expressions.
 *
 * @author Vlad Orzhekhovskiy
 */
object ScriptFunctions extends NumericFunctions with DateTimeFunctions with StringFunctions

/**
 * Numeric functions.
 */
trait NumericFunctions {

  def ABS(x: Double) = math.abs(x)
  def ABS(x: Int) = math.abs(x)
  def ABS(x: java.math.BigDecimal) = x.abs

  def FLOOR(x: Double) = math.floor(x)
  def CEIL(x: Double) = math.ceil(x)
  def ROUND(x: Double, decimals: Int) = {
    val multiplier = POW(10, decimals)
    math.round(x * multiplier).toDouble / multiplier
  }
  def ROUND(x: Double) = math.round(x)

  def POW(x: Double, y: Double) = math.pow(x, y)
  def POW(x: Int, y: Int) = List.fill(y)(x).product
  def FACTORIAL(n: Int) = (1 to n).foldLeft(1)((total, i) => total * i)
}

/**
 * DateTime functions.
 */
trait DateTimeFunctions {
  import DateTimeFunctions._

  implicit def jodaToJavaDate(dt: DateTime) = new java.sql.Date(dt.getMillis)
  implicit def jodaToJavaTimestamp(dt: DateTime) = new java.sql.Timestamp(dt.getMillis)

  def DATE(year: Int, month: Int, day: Int): java.sql.Date =
    new DateTime(year, month, day, 0, 0)
  def TIMESTAMP(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int): java.sql.Timestamp =
    new DateTime(year, month, day, hour, minute, second)
  def TIMESTAMP(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int, zone: String): java.sql.Timestamp =
    new DateTime(year, month, day, hour, minute, second, DateTimeZone.forID(zone))
  def TODAY(): java.sql.Date = DateTime.now.withTimeAtStartOfDay
  def NOW(): java.sql.Timestamp = DateTime.now

  def YEAR(x: java.util.Date): Int = YEAR(new DateTime(x))
  def MONTH(x: java.util.Date): Int = MONTH(new DateTime(x))
  def WEEK(x: java.util.Date): Int = WEEK(new DateTime(x))
  def DAY(x: java.util.Date): Int = DAY(new DateTime(x))
  def WEEKDAY(x: java.util.Date): Int = WEEKDAY(new DateTime(x))
  def HOUR(x: java.util.Date): Int = HOUR(new DateTime(x))
  def MINUTE(x: java.util.Date): Int = MINUTE(new DateTime(x))
  def SECOND(x: java.util.Date): Int = SECOND(new DateTime(x))

  def PLUS(x: java.sql.Date, count: Int, unit: String): java.sql.Date =
    new java.sql.Date(PLUS(new DateTime(x), count, unit).getMillis)

  def MINUS(x: java.sql.Date, count: Int, unit: String): java.sql.Date =
    new java.sql.Date(MINUS(new DateTime(x), count, unit).getMillis)

  private def DATEDIFF(x: java.util.Date, y: java.util.Date, unit: String): Long =
    DATEDIFF(new DateTime(x), new DateTime(y), unit)

  private def YEAR(x: DateTime): Int = x.getYear
  private def MONTH(x: DateTime): Int = x.getMonthOfYear
  private def WEEK(x: DateTime): Int = x.getWeekOfWeekyear
  private def DAY(x: DateTime): Int = x.getDayOfMonth
  private def WEEKDAY(x: DateTime): Int = x.getDayOfWeek
  private def HOUR(x: DateTime): Int = x.getHourOfDay
  private def MINUTE(x: DateTime): Int = x.getMinuteOfHour
  private def SECOND(x: DateTime): Int = x.getSecondOfMinute

  private def PLUS(x: DateTime, count: Int, unit: String): DateTime = unit match {
    case YEAR => x.plusYears(count)
    case MONTH => x.plusMonths(count)
    case DAY => x.plusDays(count)
    case HOUR => x.plusHours(count)
    case MINUTE => x.plusMinutes(count)
    case SECOND => x.plusSeconds(count)
  }

  private def MINUS(x: DateTime, count: Int, unit: String): DateTime = unit match {
    case YEAR => x.minusYears(count)
    case MONTH => x.minusMonths(count)
    case DAY => x.minusDays(count)
    case HOUR => x.minusHours(count)
    case MINUTE => x.minusMinutes(count)
    case SECOND => x.minusSeconds(count)
  }

  private def DATEDIFF(x: DateTime, y: DateTime, unit: String): Long = {
    val duration = new Duration(x, y)
    unit match {
      case DAY => duration.getStandardDays
      case HOUR => duration.getStandardHours
      case MINUTE => duration.getStandardMinutes
      case SECOND => duration.getStandardSeconds
    }
  }
}

object DateTimeFunctions {
  val YEAR = "year"
  val MONTH = "month"
  val DAY = "day"
  val HOUR = "hour"
  val MINUTE = "minute"
  val SECOND = "second"
}

/**
 * String functions.
 */
trait StringFunctions {
  def LOWER(x: String) = x.toLowerCase
  def UPPER(x: String) = x.toUpperCase

  def LEFT(x: String, count: Int) = x.take(count)
  def RIGHT(x: String, count: Int) = x.takeRight(count)
  def MID(x: String, start: Int, end: Int) = x.substring(start, end)

  def FORMAT(s: String, arg1: Any) = s.format(arg1)
  def FORMAT(s: String, arg1: Any, arg2: Any) = s.format(arg1, arg2)
  def FORMAT(s: String, arg1: Any, arg2: Any, arg3: Any) = s.format(arg1, arg2, arg3)
  def FORMAT(s: String, arg1: Any, arg2: Any, arg3: Any, arg4: Any) = s.format(arg1, arg2, arg3, arg4)
}