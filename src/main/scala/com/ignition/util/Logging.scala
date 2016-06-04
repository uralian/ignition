package com.ignition.util

import org.slf4j.LoggerFactory

/**
 * Provides logging support.
 */
trait Logging {
  private val log = LoggerFactory.getLogger(getClass)

  def trace(message: => String, args: Any*) = log.trace(message, a2o(args): _*)
  def debug(message: => String, args: Any*) = log.debug(message, a2o(args): _*)
  def info(message: => String, args: Any*) = log.info(message, a2o(args): _*)
  def warn(message: => String, args: Any*) = log.warn(message, a2o(args): _*)
  def warn(message: => String, err: Throwable) = log.warn(message, err)
  def error(message: => String, args: Any*) = log.error(message, a2o(args): _*)
  def error(message: => String, err: Throwable) = log.error(message, err)

  private def a2o(x: Seq[Any]): Seq[Object] = x.map(_.asInstanceOf[java.lang.Object])
}