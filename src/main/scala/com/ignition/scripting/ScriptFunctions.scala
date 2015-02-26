package com.ignition.scripting

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
  def DATE(year: Int, month: Int, day: Int): DateTime = new DateTime(year, month, day, 0, 0)
  def DATETIME(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int): DateTime =
    new DateTime(year, month, day, hour, minute, second)
  def DATETIME(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int, zone: String): DateTime =
    new DateTime(year, month, day, hour, minute, second, DateTimeZone.forID(zone))
  def TODAY() = DateTime.now.withTimeAtStartOfDay
  def NOW() = DateTime.now

  def YEAR(x: DateTime) = x.getYear
  def MONTH(x: DateTime) = x.getMonthOfYear
  def WEEK(x: DateTime) = x.getWeekOfWeekyear
  def DAY(x: DateTime) = x.getDayOfMonth
  def WEEKDAY(x: DateTime) = x.getDayOfWeek
  def HOUR(x: DateTime) = x.getHourOfDay
  def MINUTE(x: DateTime) = x.getMinuteOfHour
  def SECOND(x: DateTime) = x.getSecondOfMinute

  import DateTimeFunctions._
  def PLUS(x: DateTime, count: Int, unit: String) = unit match {
    case YEAR => x.plusYears(count)
    case MONTH => x.plusMonths(count)
    case DAY => x.plusDays(count)
    case HOUR => x.plusHours(count)
    case MINUTE => x.plusMinutes(count)
    case SECOND => x.plusSeconds(count)
  }
  def MINUS(x: DateTime, count: Int, unit: String) = unit match {
    case YEAR => x.minusYears(count)
    case MONTH => x.minusMonths(count)
    case DAY => x.minusDays(count)
    case HOUR => x.minusHours(count)
    case MINUTE => x.minusMinutes(count)
    case SECOND => x.minusSeconds(count)
  }

  def DATEDIFF(x: DateTime, y: DateTime, unit: String) = {
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