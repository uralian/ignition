package com.ignition.data

import org.joda.time.DateTime

import com.eaio.uuid.UUID

/**
 * Encapsulates a list of data items with conversion functions for the supported data types.
 *
 * @author Vlad Orzhekhovskiy
 */
case class DataRow(columnNames: IndexedSeq[String], data: IndexedSeq[Any]) {

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

  def get[T](index: Int)(implicit dataType: DataType[T]): T = dataType.convert(data(index))
  def get[T](name: String)(implicit dataType: DataType[T]): T = get[T](columnNames.indexOf(name))
}