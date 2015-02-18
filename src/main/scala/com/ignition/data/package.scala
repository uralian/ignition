package com.ignition

import org.joda.time.DateTime

import com.eaio.uuid.UUID

/**
 * Data types implicits and aliases.
 *
 * @author Vlad Orzhekhovskiy
 */
package object data {

  type Binary = Array[Byte]
  type Decimal = BigDecimal

  /* column info and row metadata builders */
  def boolean(name: String) = ColumnInfo[Boolean](name)
  def string(name: String) = ColumnInfo[String](name)
  def int(name: String) = ColumnInfo[Int](name)
  def double(name: String) = ColumnInfo[Double](name)
  def decimal(name: String) = ColumnInfo[Decimal](name)
  def datetime(name: String) = ColumnInfo[DateTime](name)
  def uuid(name: String) = ColumnInfo[UUID](name)
  def binary(name: String) = ColumnInfo[Binary](name)

  implicit def columnInfo2metaData(ci: ColumnInfo[_]): DefaultRowMetaData = DefaultRowMetaData(ci)
  
  /* data row builder */
  implicit class RichStringList(val names: Vector[String]) extends AnyVal {
    def ~(name: String): RichStringList = new RichStringList(names :+ name)
    def row(raw: IndexedSeq[Any]): DefaultDataRow = DefaultDataRow(names, raw)
    def row(raw: Any*): DefaultDataRow = row(raw.toIndexedSeq)
  }
  
  implicit def string2richList(str: String): RichStringList = new RichStringList(Vector(str))
}