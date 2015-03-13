package com.ignition.workflow.rdd.grid.pair

import scala.xml.{ Elem, Node }
import com.ignition.data.{ DataRow, DefaultRowMetaData, RowMetaData }
import com.ignition.workflow.rdd.grid.XmlFactory
import scala.xml.Utility

/**
 * Converts a data row into some arbitrary class U, which is subsequently used in Spark
 * aggregateByKey method. Once the aggregation is done, the resulting value of type U is
 * converted back to a data row:
 *
 * DataRow -convert-> U -aggregateByKey-> U -invert-> DataRow
 */
trait RowAggregator[U] extends Serializable {
  /**
   * Converts a data row into a value.
   */
  def convert(row: DataRow): U
  /**
   * Provides the "zero value" for aggregation.
   */
  def zero: U
  /**
   * Merges a data row into the value.
   */
  def seqOp(value: U, row: DataRow): U
  /**
   * Combines the two values.
   */
  def combOp(value1: U, value2: U): U
  /**
   * Converts the value back to a data row.
   */
  def invert(value: U): DataRow
  /**
   * Provides the meta data for the returned row.
   */
  def outMetaData: RowMetaData
  /**
   * Converts the aggregator to XML.
   */
  def toXml: Elem
}

object RowAggregator extends XmlFactory[RowAggregator[_]] {
  def fromXml(xml: Node) = Utility.trim(xml) match {
    case n @ <field-list-aggregator>{ _* }</field-list-aggregator> => FieldListAggregator.fromXml(n)
  }
}

/**
 * An implementation of RowAggregator, which aggregates individual fields independently.
 */
case class FieldListAggregator(aggrs: Iterable[FieldAggregator[_, _]]) extends RowAggregator[Iterable[Any]] {

  private val anyAggrs = aggrs map (_.asInstanceOf[FieldAggregator[Any, Any]])

  def convert(row: DataRow) = aggrs map (fa => row.getRaw(fa.columnName))

  def zero = aggrs map (_.zero)

  def seqOp(list: Iterable[Any], row: DataRow) = (list zip anyAggrs) map {
    case (value, fa) => fa.seqOp(value, row.getRaw(fa.columnName))
  }

  def combOp(list1: Iterable[Any], list2: Iterable[Any]) = (anyAggrs zip (list1 zip list2)) map {
    case (fa, (value1, value2)) => fa.combOp(value1, value2)
  }

  def invert(list: Iterable[Any]) = {
    val data = (list zip anyAggrs) flatMap {
      case (value, fa) => fa.invert(value)
    }
    outMetaData.row(data.toIndexedSeq)
  }

  def outMetaData = DefaultRowMetaData(aggrs flatMap (_.outMetaData.columns) toIndexedSeq)

  def toXml: Elem = <field-list-aggregator>{ aggrs map (_.toXml) }</field-list-aggregator>
}

object FieldListAggregator extends XmlFactory[FieldListAggregator] {
  def apply(aggrs: FieldAggregator[_, _]*): FieldListAggregator = apply(aggrs.toSeq)
  def fromXml(xml: Node) = {
    val fas = xml.child filter (_.isInstanceOf[Elem]) map Aggregators.fromXml
    FieldListAggregator(fas)
  }
}