package com.ignition.data

import org.joda.time.DateTime

import com.eaio.uuid.UUID
import com.ignition.data.DataType.{ BinaryDataType, BooleanDataType, DateTimeDataType, DecimalDataType, DoubleDataType, IntDataType, StringDataType, UUIDDataType }

/**
 * Encapsulates a list of data items with extraction functions for the supported data types.
 * All get...() method throw TypeConversionException if the conversion fails.
 *
 * @author Vlad Orzhekhovskiy
 */
trait DataRow extends Serializable {
  def columnNames: IndexedSeq[String]
  def rawData: IndexedSeq[Any]
  def get[T](index: Int)(implicit dataType: DataType[T]): T

  def columnCount: Int = columnNames.size
  def get[T](name: String)(implicit dataType: DataType[T]): T = get[T](columnNames.indexOf(name))

  def getRaw(index: Int): Any = rawData(index)
  def getRaw(name: String): Any = getRaw(columnNames.indexOf(name))

  def getString(index: Int) = get[String](index)
  def getString(name: String) = get[String](name)

  def getBoolean(index: Int) = get[Boolean](index)
  def getBoolean(name: String) = get[Boolean](name)

  def getInt(index: Int) = get[Int](index)
  def getInt(name: String) = get[Int](name)

  def getDouble(index: Int) = get[Double](index)
  def getDouble(name: String) = get[Double](name)

  def getDecimal(index: Int) = get[BigDecimal](index)
  def getDecimal(name: String) = get[BigDecimal](name)

  def getDateTime(index: Int) = get[DateTime](index)
  def getDateTime(name: String) = get[DateTime](name)

  def getBinary(index: Int) = get[Array[Byte]](index)
  def getBinary(name: String) = get[Array[Byte]](name)

  def getUUID(index: Int) = get[UUID](index)
  def getUUID(name: String) = get[UUID](name)
}

/**
 * The default implementation of DataRow backed by indexed sequences for column names and data.
 * The sizes of the name list and data list should match.
 *
 * @author Vlad Orzhekhovskiy
 */
case class DefaultDataRow(columnNames: IndexedSeq[String], rawData: IndexedSeq[Any]) extends DataRow {

  require(columnNames.size == rawData.size, "Column name count and data count do not match")
  require(columnNames.map(_.toLowerCase).toSet.size == columnNames.size, "Duplicate column name")

  def get[T](index: Int)(implicit dataType: DataType[T]): T = dataType.convert(rawData(index))

  def row(columnNames: Iterable[String]) = DefaultDataRow.subrow(this, columnNames)
}

/**
 * DataRow companion object.
 */
object DefaultDataRow {
  /**
   * Extracts only the specified columns from the row.
   */
  def subrow(row: DataRow, columnNames: Iterable[String]) = {
    val data = columnNames.toIndexedSeq map row.getRaw
    DefaultDataRow(columnNames.toIndexedSeq, data)
  }
}