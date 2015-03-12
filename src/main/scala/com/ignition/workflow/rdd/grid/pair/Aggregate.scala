package com.ignition.workflow.rdd.grid.pair

import scala.reflect.ClassTag
import scala.xml.{ Elem, Node }

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

import com.ignition.data.{ DataRow, DefaultDataRow, DefaultRowMetaData, RowMetaData }
import com.ignition.util.XmlUtils.RichNodeSeq
import com.ignition.workflow.rdd.grid.{ GridStep1, XmlFactory }

/**
 * Implements "aggregate-by-key" functionality. Receives the list of keys and the row
 * aggregator at the construction time. At runtime, converts an RDD into (keys, fields)
 * pair RDD and invokes aggregator to perform the aggregation.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Aggregate[U: ClassTag](keys: Iterable[String], aggregator: RowAggregator[U]) extends GridStep1 {
  assert(keys.size > 0, "Empty key set")
  aggregator.outMetaData.columnNames foreach (name =>
    assert(!keys.exists(_ == name), s"Aggregator metadata contains a key column: $name"))

  protected def computeRDD(rdd: RDD[DataRow]): RDD[DataRow] = {
    assert(outMetaData.isDefined, "Input is not connected")

    val keys = this.keys
    val aggregator = this.aggregator

    val paired = rdd map { row => (DefaultDataRow.subrow(row, keys), row) }
    val result = paired.aggregateByKey(aggregator.zero)(aggregator.seqOp, aggregator.combOp)
    result map { case (keyrow, data) => keyrow ~~ aggregator.invert(data) }
  }

  def outMetaData: Option[RowMetaData] = inMetaData map { md =>
    keys foreach { key => assert(md.columnNames.contains(key), s"Input does not contain a key column: $key") }
    DefaultRowMetaData(keys.toIndexedSeq map md.apply) ~~ aggregator.outMetaData
  }

  def toXml: Elem =
    <aggregate>
      <keys>{ keys map (k => <key>{ k }</key>) }</keys>
      { aggregator.toXml }
    </aggregate>

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}

/**
 * Aggregate companion object.
 */
object Aggregate extends XmlFactory[Aggregate[_]] {
  def fromXml(xml: Node) = {
    val keys = (xml \\ "key") map (_.asString)
    val aggrNode = xml.child filter (node => node.isInstanceOf[Elem] && node.label != "keys") head
    val aggregator = RowAggregator.fromXml(aggrNode)
    Aggregate(keys, aggregator)
  }
}