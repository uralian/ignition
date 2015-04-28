package com.ignition.script

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.ignition.types.TypeUtils

/**
 * Row expression processor. It evaluates a data row and returns the result.
 *
 * @author Vlad Orzhekhovskiy
 */
trait RowExpression[T <: DataType] extends Serializable {
  /**
   * The static return type of the expression. If None, this means that it cannot be
   * determined statically.
   */
  def targetType: Option[T]

  /**
   * Computes the dynamic return type of the expression.
   */
  def computeTargetType(schema: StructType) = evaluate(schema) _ andThen TypeUtils.typeForValue

  /**
   * Evaluates the data row and computes the result.
   */
  def evaluate(schema: StructType)(row: Row): Any
}