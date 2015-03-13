package com.ignition.workflow.rdd.grid.pair

import scala.xml.{ Elem, Node, Utility }

import org.apache.spark.util.StatCounter

import com.ignition.data.{ ColumnInfo, DataType, RowMetaData, columnInfo2metaData, double, int, string }
import com.ignition.util.XmlUtils.RichNodeSeq
import com.ignition.workflow.rdd.grid.XmlFactory

/**
 * A collection of supported field aggregators.
 *
 * @author Vlad Orzhekhovskiy
 */
object Aggregators {

  def fromXml(xml: Node) = Utility.trim(xml) match {
    case n @ <mk-string>{ _* }</mk-string> => MkString.fromXml(n)
    case n @ <basic-stats>{ _* }</basic-stats> => BasicStats.fromXml(n)
  }

  /**
   * Concatenates string values of the specified column.
   */
  case class MkString(override val columnName: String, separator: String = ",", prefix: String = "", suffix: String = "")
    extends FieldAggregator[String, String](columnName, "") {

    def seqOp(total: String, raw: String) = total + separator + raw

    def combOp(str1: String, str2: String) = str1 + separator + str2

    def invert(str: String) = Vector(prefix + str + suffix)

    val outMetaData: RowMetaData = string(columnName)

    def toXml: Elem =
      <mk-string name={ columnName }>
        <separator>{ separator }</separator>
        <prefix>{ prefix }</prefix>
        <suffix>{ suffix }</suffix>
      </mk-string>
  }

  object MkString extends XmlFactory[MkString] {
    def fromXml(xml: Node) = {
      val columnName = (xml \ "@name").asString
      val separator = (xml \ "separator").asString
      val prefix = (xml \ "prefix").asString
      val suffix = (xml \ "suffix").asString
      MkString(columnName, separator, prefix, suffix)
    }
  }

  /**
   * Computes basic stats for a numeric column.
   */
  case class BasicStats[T](override val columnName: String)(implicit dt: DataType[T])
    extends FieldAggregator[T, StatCounter](columnName, StatCounter()) {

    private val dblType = implicitly[DataType[Double]]

    def seqOp(sc: StatCounter, raw: T) = sc merge dblType.convert(raw)

    def combOp(sc1: StatCounter, sc2: StatCounter) = sc1 merge sc2

    def invert(sc: StatCounter) = Vector(sc.count.toInt, dt.convert(sc.min), dt.convert(sc.max), dt.convert(sc.sum), sc.mean)

    val outMetaData = int(%("cnt")) ~
      ColumnInfo(%("min"))(dt) ~
      ColumnInfo(%("max"))(dt) ~
      ColumnInfo(%("sum"))(dt) ~
      double(%("avg"))

    private def %(suffix: String) = columnName + "_" + suffix

    def toXml: Elem = <basic-stats name={ columnName } type={ dt.code }/>
  }

  object BasicStats extends XmlFactory[BasicStats[_]] {
    def fromXml(xml: Node) = {
      val columnName = (xml \ "@name").asString
      val dataType = DataType.withCode((xml \ "@type").text)
      BasicStats(columnName)(dataType)
    }
  }
}