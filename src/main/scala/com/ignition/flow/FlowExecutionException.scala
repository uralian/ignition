package com.ignition.flow

/**
 * Workflow execution exception.
 *
 * @author Vlad Orzhekhovskiy
 */
trait FlowExecutionException extends RuntimeException

/**
 * Workflow execution exception companion object to create new instances.
 */
object FlowExecutionException {
  def apply() =
    new RuntimeException with FlowExecutionException

  def apply(message: String) =
    new RuntimeException(message) with FlowExecutionException

  def apply(message: String, cause: Throwable) =
    new RuntimeException(message, cause) with FlowExecutionException

  def apply(cause: Throwable) =
    new RuntimeException(cause) with FlowExecutionException

  def apply(message: String, cause: Throwable, enableSuppression: Boolean, writableStackTrace: Boolean) =
    new RuntimeException(message, cause, enableSuppression, writableStackTrace) with FlowExecutionException
}