package com.ignition.workflow.rdd.grid.pair

import scala.reflect.ClassTag
import com.ignition.data.{ DataType, RowMetaData }
import scala.xml.Elem

/**
 * Extracts a field from a data row and converts it into some arbitrary value U, as part of
 * the RowAggregator workflow. Once the aggregation is done, the resulting value of type U is
 * converted back to sequence of values:
 *
 * DataRow -extract-> U -aggregateByKey-> U -invert->(Iterable[Any] + metadata)
 */
abstract class FieldAggregator[T, U: ClassTag](val columnName: String, val zero: U)(implicit dt: DataType[T]) extends Serializable {
  /**
   * Merges a raw field data into the cumulative value.
   */
  def seqOp(value: U, raw: T): U
  /**
   * Combines two cumulative values.
   */
  def combOp(value1: U, value2: U): U
  /**
   * Converts a cumulative value into a sequence of raw values.
   */
  def invert(value: U): Iterable[Any]
  /**
   * Builds a return meta data.
   */
  val outMetaData: RowMetaData
  /**
   * Converts aggregator to XML.
   */
  def toXml: Elem
}