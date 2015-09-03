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

    def getAsString = self match {
      case JString(s) => Some(s)
      case _          => None
    }

    def getAsBigInt = self match {
      case JInt(bi) => Some(bi)
      case _        => None
    }

    def getAsInt = self match {
      case JInt(bi) => Some(bi.toInt)
      case _        => None
    }

    def getAsLong = self match {
      case JInt(bi) => Some(bi.toLong)
      case _        => None
    }

    def getAsDouble = self match {
      case JDouble(d)   => Some(d)
      case JInt(n)      => Some(n.toDouble)
      case JDecimal(bd) => Some(bd.toDouble)
      case _            => None
    }

    def getAsDecimal = self match {
      case JDecimal(bd) => Some(bd)
      case JInt(n)      => Some(BigDecimal(n))
      case JDouble(d)   => Some(BigDecimal(d))
      case _            => None
    }

    def getAsBoolean = self match {
      case JBool(b) => Some(b)
      case _        => None
    }

    /* primitives; null if does not exist */

    def asString = getAsString orNull

    def asBigInt = getAsBigInt orNull

    def asInt = getAsInt getOrElse 0

    def asLong = getAsLong getOrElse 0L

    def asDouble = getAsDouble getOrElse 0.0

    def asDecimal = getAsDecimal orNull

    def asBoolean = getAsBoolean getOrElse false

    /* collections; Nil if does not exist */

    def asArray = self match {
      case JArray(items) => items
      case _             => Nil
    }
  }
}