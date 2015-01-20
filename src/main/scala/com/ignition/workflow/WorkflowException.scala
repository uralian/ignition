package com.ignition.workflow

/**
 * Workflow exception.
 *
 * @author Vlad Orzhekhovskiy
 */
trait WorkflowException extends RuntimeException

/**
 * Workflow exception companion object to create new instances.
 */
object WorkflowException {
  def apply() =
    new RuntimeException with WorkflowException

  def apply(message: String) =
    new RuntimeException(message) with WorkflowException

  def apply(message: String, cause: Throwable) =
    new RuntimeException(message, cause) with WorkflowException

  def apply(cause: Throwable) =
    new RuntimeException(cause) with WorkflowException

  def apply(message: String, cause: Throwable, enableSuppression: Boolean, writableStackTrace: Boolean) =
    new RuntimeException(message, cause, enableSuppression, writableStackTrace) with WorkflowException
}