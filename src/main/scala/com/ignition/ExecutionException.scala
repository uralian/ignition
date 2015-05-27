package com.ignition

/**
 * Workflow execution exception.
 *
 * @author Vlad Orzhekhovskiy
 */
trait ExecutionException extends RuntimeException

/**
 * Workflow execution exception companion object to create new instances.
 */
object ExecutionException {
  def apply() =
    new RuntimeException with ExecutionException

  def apply(message: String) =
    new RuntimeException(message) with ExecutionException

  def apply(message: String, cause: Throwable) =
    new RuntimeException(message, cause) with ExecutionException

  def apply(cause: Throwable) =
    new RuntimeException(cause) with ExecutionException

  def apply(message: String, cause: Throwable, enableSuppression: Boolean, writableStackTrace: Boolean) =
    new RuntimeException(message, cause, enableSuppression, writableStackTrace) with ExecutionException
}