package com.ignition.workflow.rdd.grid.pair

import scala.Array.canBuildFrom
import scala.annotation.elidable
import scala.annotation.elidable.ASSERTION
import scala.xml.{ Elem, Node }

import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

import com.eaio.uuid.UUID
import com.ignition.data.{ Binary, DataRow, DataType }
import com.ignition.data.{ Decimal, DefaultDataRow, DefaultRowMetaData, RichBoolean, RowMetaData }
import com.ignition.data.DataType.{ BinaryDataType, BooleanDataType, DateTimeDataType, DecimalDataType, DoubleDataType, IntDataType, StringDataType, UUIDDataType }
import com.ignition.util.XmlUtils.RichNodeSeq
import com.ignition.workflow.rdd.grid.{ GridStep1, XmlFactory }

/**
 * Reduce operations
 */
object ReduceOp extends Enumeration {
  abstract class ReduceOp extends super.Val {
    def reduce[T](a: T, b: T)(implicit dt: DataType[T]): T
  }
  implicit def valueToOp(v: Value) = v.asInstanceOf[ReduceOp]

  val ANY = new ReduceOp { def reduce[T](a: T, b: T)(implicit dt: DataType[T]): T = a }

  val SUM = new ReduceOp {
    def reduce[T](a: T, b: T)(implicit dt: DataType[T]): T = dt match {
      case BooleanDataType => a || b
      case StringDataType => a + b
      case IntDataType => a + b
      case DoubleDataType => a + b
      case DecimalDataType => a + b
      case BinaryDataType => a ++ b
      case _ => throw new IllegalArgumentException(s"SUM not defined for type ${dt.code}")
    }
  }

  val MIN = new ReduceOp {
    def reduce[T](a: T, b: T)(implicit dt: DataType[T]): T = dt match {
      case BooleanDataType => (a < b) ? (a, b)
      case StringDataType => (a < b) ? (a, b)
      case IntDataType => (a < b) ? (a, b)
      case DoubleDataType => (a < b) ? (a, b)
      case DecimalDataType => (a < b) ? (a, b)
      case DateTimeDataType => a.isBefore(b) ? (a, b)
      case _ => throw new IllegalArgumentException(s"MIN not defined for type ${dt.code}")
    }
  }

  val MAX = new ReduceOp {
    def reduce[T](a: T, b: T)(implicit dt: DataType[T]): T = dt match {
      case BooleanDataType => (a > b) ? (a, b)
      case StringDataType => (a > b) ? (a, b)
      case IntDataType => (a > b) ? (a, b)
      case DoubleDataType => (a > b) ? (a, b)
      case DecimalDataType => (a > b) ? (a, b)
      case DateTimeDataType => a.isAfter(b) ? (a, b)
      case _ => throw new IllegalArgumentException(s"MAX not defined for type ${dt.code}")
    }
  }
}
import ReduceOp._

/**
 * Reduces the RDD by grouping rows by key columns and then running reduce() on the selected
 * fields.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Reduce(keys: Set[String], ops: Map[String, ReduceOp]) extends GridStep1 {
  assert(keys.size > 0, "Empty key set")
  assert(ops.size > 0, "Empty operation set")

  ops.keys foreach (field => assert(!keys.contains(field), s"Reduced fields cannot be keys: $field"))

  protected def computeRDD(rdd: RDD[DataRow]): RDD[DataRow] = {
    assert(outMetaData.isDefined, "Input is not connected")

    val keys = this.keys
    val ops = this.ops

    val paired = rdd map { row =>
      (DefaultDataRow.subrow(row, keys), DefaultDataRow.subrow(row, ops.keys))
    }

    val columns = inMetaData.get.columns filter (ci => ops.keySet.contains(ci.name)) map { ci =>
      ops(ci.name)
    }

    val result = paired reduceByKey { (row1, row2) =>
      assert(row1.columnNames == row2.columnNames)
      val data = (columns zip (row1.rawData zip row2.rawData)) map {
        case (op, (a: Boolean, b: Boolean)) => op.reduce(a, b)
        case (op, (a: String, b: String)) => op.reduce(a, b)
        case (op, (a: Int, b: Int)) => op.reduce(a, b)
        case (op, (a: Double, b: Double)) => op.reduce(a, b)
        case (op, (a: Decimal, b: Decimal)) => op.reduce(a, b)
        case (op, (a: DateTime, b: DateTime)) => op.reduce(a, b)
        case (op, (a: UUID, b: UUID)) => op.reduce(a, b)
        case (op, (a: Binary, b: Binary)) => op.reduce(a, b)
      }
      DefaultDataRow(row1.columnNames, data)
    }

    val meta = outMetaData.get
    result map {
      case (keyRow, fieldRow) =>
        val data = meta.columnNames map { name =>
          if (keys.contains(name))
            keyRow.getRaw(name)
          else
            fieldRow.getRaw(name)
        }
        DefaultDataRow(meta.columnNames, data)
    }
  }

  def outMetaData: Option[RowMetaData] = inMetaData map { md =>
    keys foreach { key => assert(md.columnNames.contains(key), s"Input does not contain a key column: $key") }
    ops.keys foreach { fld => assert(md.columnNames.contains(fld), s"Input does not contain a data column: $fld") }

    val columns = md.columns filter (ci => keys.contains(ci.name) || ops.keySet.contains(ci.name))
    DefaultRowMetaData(columns)
  }

  def toXml: Elem =
    <reduce>
      <keys>
        { keys map (k => <key>{ k }</key>) }
      </keys>
      <operations>
        {
          ops map {
            case (name, op) => <field name={ name } op={ op.toString }/>
          }
        }
      </operations>
    </reduce>

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}

/**
 * Reduce companion object.
 */
object Reduce extends XmlFactory[Reduce] {

  def apply(key: String, ops: (String, ReduceOp)*): Reduce = apply(Set(key), ops.toMap)

  def apply(key1: String, key2: String, ops: (String, ReduceOp)*): Reduce =
    apply(Set(key1, key2), ops.toMap)

  def fromXml(xml: Node) = {
    val keys = (xml \ "keys" \ "key") map (_.asString)
    val ops = (xml \ "operations" \ "field") map { node =>
      val name = (node \ "@name").asString
      val op = ReduceOp.withName((node \ "@op").asString): ReduceOp
      name -> op
    }
    Reduce(keys.toSet, ops.toMap)
  }
}