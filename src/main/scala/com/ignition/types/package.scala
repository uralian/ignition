package com.ignition

import org.apache.spark.sql.types._

/**
 * Data types, implicits, aliases for DataFrame-based workflows.
 *
 * @author Vlad Orzhekhovskiy
 */
package object types {

  type Binary = Array[Byte]
  val DecType = DecimalType.Unlimited

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
  }

  implicit def fieldToStruct(field: StructField): RichStructType = StructType(Array(field))
}