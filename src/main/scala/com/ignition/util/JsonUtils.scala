package com.ignition.util

import org.json4s.{ JArray, JBool, JDecimal, JDouble, JInt, JString, JValue }

/**
 * A collection of helper functions for processing JSON objects.
 */
object JsonUtils {

  /**
   * Adds typed functionality for JValue.
   */
  implicit class RichJValue(val self: JValue) extends AnyVal {

    /* primitive options; None if does not exist */

    def asStringOption = self match {
      case JString(s) => Some(s)
      case _          => None
    }

    def asBigIntOption = self match {
      case JInt(bi) => Some(bi)
      case _        => None
    }

    def asIntOption = self match {
      case JInt(bi) => Some(bi.toInt)
      case _        => None
    }

    def asLongOption = self match {
      case JInt(bi) => Some(bi.toLong)
      case _        => None
    }

    def asDoubleOption = self match {
      case JDouble(d)   => Some(d)
      case JInt(n)      => Some(n.toDouble)
      case JDecimal(bd) => Some(bd.toDouble)
      case _            => None
    }

    def asDecimalOption = self match {
      case JDecimal(bd) => Some(bd)
      case JInt(n)      => Some(BigDecimal(n))
      case JDouble(d)   => Some(BigDecimal(d))
      case _            => None
    }

    def asBooleanOption = self match {
      case JBool(b) => Some(b)
      case _        => None
    }

    /* primitives; null if does not exist */

    def asString = asStringOption orNull

    def asBigInt = asBigIntOption orNull

    def asInt = asIntOption getOrElse 0

    def asLong = asLongOption getOrElse 0L

    def asDouble = asDoubleOption getOrElse 0.0

    def asDecimal = asDecimalOption orNull

    def asBoolean = asBooleanOption getOrElse false

    /* collections; Nil if does not exist */

    def asArray = self match {
      case JArray(items) => items
      case _             => Nil
    }
  }
}