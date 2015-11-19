package com.ignition

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

/**
 * Data types, implicits, aliases for DataFrame-based workflows.
 *
 * @author Vlad Orzhekhovskiy
 */
package object types {

  type Binary = Array[Byte]
  val DecType = DecimalType.SYSTEM_DEFAULT

  /* schema field builders */

  def binary(name: String, nullable: Boolean = true) = StructField(name, BinaryType, nullable)
  def boolean(name: String, nullable: Boolean = true) = StructField(name, BooleanType, nullable)
  def string(name: String, nullable: Boolean = true) = StructField(name, StringType, nullable)
  def byte(name: String, nullable: Boolean = true) = StructField(name, ByteType, nullable)
  def short(name: String, nullable: Boolean = true) = StructField(name, ShortType, nullable)
  def int(name: String, nullable: Boolean = true) = StructField(name, IntegerType, nullable)
  def long(name: String, nullable: Boolean = true) = StructField(name, LongType, nullable)
  def float(name: String, nullable: Boolean = true) = StructField(name, FloatType, nullable)
  def double(name: String, nullable: Boolean = true) = StructField(name, DoubleType, nullable)
  def decimal(name: String, nullable: Boolean = true) = StructField(name, DecType, nullable)
  def date(name: String, nullable: Boolean = true) = StructField(name, DateType, nullable)
  def timestamp(name: String, nullable: Boolean = true) = StructField(name, TimestampType, nullable)

  /* struct type enhancements */

  implicit class RichStructType(val schema: StructType) extends AnyVal {
    def addField(field: StructField) = schema.copy(fields = schema.fields :+ field)
    def ~(field: StructField) = addField(field)
    def addFields(fields: Seq[StructField]) = schema.copy(fields = schema.fields ++ fields)
    def ~~(fields: Seq[StructField]) = addFields(fields)
    def toRowHelper = RowSchemaHelper(schema)
    def indexMap = toRowHelper indexMap
  }

  implicit def fieldToRichStruct(field: StructField): RichStructType = StructType(Array(field))
  implicit def fieldToStructType(field: StructField): StructType = StructType(Array(field))

  /* Row enhancemenets */

  implicit class RichRow(val row: Row) extends AnyVal {
    def get(implicit rsh: RowSchemaHelper) = rsh.get(row)
    def isNullAt(implicit rsh: RowSchemaHelper) = rsh.isNullAt(row)
    def getBoolean(implicit rsh: RowSchemaHelper) = rsh.getBoolean(row)
    def getByte(implicit rsh: RowSchemaHelper) = rsh.getByte(row)
    def getShort(implicit rsh: RowSchemaHelper) = rsh.getShort(row)
    def getInt(implicit rsh: RowSchemaHelper) = rsh.getInt(row)
    def getLong(implicit rsh: RowSchemaHelper) = rsh.getLong(row)
    def getFloat(implicit rsh: RowSchemaHelper) = rsh.getFloat(row)
    def getDouble(implicit rsh: RowSchemaHelper) = rsh.getDouble(row)
    def getString(implicit rsh: RowSchemaHelper) = rsh.getString(row)
    def getDecimal(implicit rsh: RowSchemaHelper) = rsh.getDecimal(row)
    def getDate(implicit rsh: RowSchemaHelper) = rsh.getDate(row)
    def getTimestamp(implicit rsh: RowSchemaHelper) = rsh.getTimestamp(row)

    def subrow(indices: Int*): Row = {
      val data = indices map row.get toArray
      val schema = StructType(indices map row.schema)
      new GenericRowWithSchema(data, schema)
    }
    
    def subrow(names: String*)(implicit rsh: RowSchemaHelper): Row = {
      val indices = names map rsh.indexMap.apply
      subrow(indices: _*)
    }
  }

  /* boolean to option etc. */
  implicit class RichBoolean(val b: Boolean) extends AnyVal {
    final def option[A](a: => A): Option[A] = if (b) Some(a) else None
    final def ?[T](t: => T, f: => T): T = if (b) t else f
  }
}