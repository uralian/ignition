package com.ignition.workflow.rdd.grid.pair

import scala.xml.{ Elem, Node }

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD

import com.ignition.data.{ DataRow, DefaultDataRow, DefaultRowMetaData, RowMetaData }
import com.ignition.util.XmlUtils.{ RichNodeSeq, intToText, optToOptText }
import com.ignition.workflow.rdd.grid.{ GridStep2, XmlFactory }

/**
 * Join type.
 */
object JoinType extends Enumeration {
  type JoinType = Value

  val INNER = Value("inner")
  val LEFT_OUTER = Value("left")
  val RIGHT_OUTER = Value("right")
  val FULL_OUTER = Value("full")
}
import JoinType._

/**
 * Performs LEFT, RIGHT, INNER, OR FULL OUTER JOIN between two RDD rows.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Join(keys: Set[String], joinType: JoinType = INNER, numPartitions: Option[Int] = None) extends GridStep2 {
  import Join._

  assert(keys.size > 0, "Empty key set")

  protected def computeRDD(rdd1: RDD[DataRow], rdd2: RDD[DataRow]): RDD[DataRow] = {
    assert(outMetaData.isDefined, "Input1 or Input2 is not connected")

    val keys = this.keys

    val paired1 = rdd1 map { row => (DefaultDataRow.subrow(row, keys), row) }
    val paired2 = rdd2 map { row => (DefaultDataRow.subrow(row, keys), row) }

    val columns2 = inMetaData.get._2.columnNames filterNot (keys.contains)
    val columnNames = inMetaData.get._1.columnNames ++ columns2

    val dataRDD = joinType match {
      case INNER => paired1 join paired2 map {
        case (_, (row1, row2)) => combineData(Some(row1), Some(row2), columns2, columnNames)
      }
      case LEFT_OUTER => paired1 leftOuterJoin paired2 map {
        case (_, (row1, None)) => combineData(Some(row1), None, columns2, columnNames)
        case (_, (row1, Some(row2))) => combineData(Some(row1), Some(row2), columns2, columnNames)
      }
      case RIGHT_OUTER => paired1 rightOuterJoin paired2 map {
        case (_, (None, row2)) => combineData(None, Some(row2), columns2, columnNames)
        case (_, (Some(row1), row2)) => combineData(Some(row1), Some(row2), columns2, columnNames)
      }
      case FULL_OUTER => paired1 fullOuterJoin paired2 map {
        case (_, (Some(row1), Some(row2))) => combineData(Some(row1), Some(row2), columns2, columnNames)
        case (_, (Some(row1), None)) => combineData(Some(row1), None, columns2, columnNames)
        case (_, (None, Some(row2))) => combineData(None, Some(row2), columns2, columnNames)
        case (_, (None, None)) => combineData(None, None, columns2, columnNames)
      }
    }

    dataRDD map (DefaultDataRow(columnNames, _))
  }

  def outMetaData: Option[RowMetaData] = inMetaData map { mds =>
    val meta1 = mds._1
    val meta2 = mds._2

    keys foreach { key =>
      assert(meta1.columnNames.contains(key), s"Input1 does not contain a key column: $key")
      assert(meta2.columnNames.contains(key), s"Input2 does not contain a key column: $key")
      assert(meta1(key) == meta2(key), s"Input types of column [$key] are different")
    }

    val columns2 = meta2.columnNames filterNot (keys.contains) map meta2.apply
    DefaultRowMetaData(meta1.columns) ~~ columns2
  }

  def toXml: Elem =
    <join type={ joinType.toString } partitions={ numPartitions }>
      { keys map (k => <key>{ k }</key>) }
    </join>

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}

/**
 * Join companion object.
 */
object Join extends XmlFactory[Join] {
  def apply(keys: String*): Join = apply(Set(keys: _*))

  def fromXml(xml: Node) = {
    val joinType = JoinType.withName((xml \ "@type").asString)
    val numPartitions = (xml \ "@partitions").getAsInt
    val keys = (xml \ "key") map (_.asString)
    Join(keys.toSet, joinType, numPartitions)
  }

  def combineData(row1: Option[DataRow], row2: Option[DataRow], columns2: IndexedSeq[String],
    columnNames: IndexedSeq[String]) = (row1, row2) match {
    case (Some(r1), Some(r2)) => r1.rawData ++ (columns2 map r2.getRaw)
    case (Some(r1), None) => r1.rawData ++ List.fill(columns2.size)(null)
    case (None, Some(r2)) => columnNames map { name =>
      val index = r2.columnNames.indexOf(name)
      if (index >= 0) r2.getRaw(index) else null
    }
    case (None, None) => throw new IllegalStateException("Empty join (should never happen)")
  }
}