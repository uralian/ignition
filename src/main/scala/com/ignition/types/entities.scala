package com.ignition.types

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

/**
 * Provides the conversion of field names to field indices.
 */
case class RowSchemaHelper(schema: StructType) {
  val indexMap = schema.fieldNames.zipWithIndex.toMap
  val fieldNames = schema.fieldNames

  def get(row: Row) = indexMap.apply _ andThen row.get
  def isNullAt(row: Row) = indexMap.apply _ andThen row.isNullAt
  def getBoolean(row: Row) = indexMap.apply _ andThen row.getBoolean
  def getByte(row: Row) = indexMap.apply _ andThen row.getByte
  def getShort(row: Row) = indexMap.apply _ andThen row.getShort
  def getInt(row: Row) = indexMap.apply _ andThen row.getInt
  def getLong(row: Row) = indexMap.apply _ andThen row.getLong
  def getFloat(row: Row) = indexMap.apply _ andThen row.getFloat
  def getDouble(row: Row) = indexMap.apply _ andThen row.getDouble
  def getString(row: Row) = indexMap.apply _ andThen row.getString
  def getDecimal(row: Row) = indexMap.apply _ andThen row.getDecimal
  def getDate(row: Row) = indexMap.apply _ andThen row.getDate
  def getTimestamp(row: Row) = indexMap.apply _ andThen row.getAs[java.sql.Timestamp]
}