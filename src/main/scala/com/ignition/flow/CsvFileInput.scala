package com.ignition.flow

import scala.util.Try
import scala.util.control.NonFatal

import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.types.{ DataType, StructType }

import com.ignition.types.TypeUtils.valueOf

/**
 * Reads CSV files.
 *
 * @author Vlad Orzhekhovskiy
 */
case class CsvFileInput(path: String, separator: String, schema: StructType) extends Producer {
  import CsvFileInput._

  def separator(sep: String) = copy(separator = sep)

  protected def compute(limit: Option[Int])(implicit ctx: SQLContext): DataFrame = {
    val schema = this.schema
    val separator = this.separator

    val rdd = ctx.sparkContext.textFile(path) map { line =>
      val arr = line.split(separator) zip schema map {
        case (str, field) => convert(str, field.dataType, field.nullable)
      }
      Row.fromSeq(arr)
    }

    val df = ctx.createDataFrame(rdd, schema)
    optLimit(df, limit)
  }

  protected def computeSchema(implicit ctx: SQLContext): StructType = schema

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}

/**
 * CSV Input companion object.
 */
object CsvFileInput {

  /**
   * Creates a new CsvFileInput step with "," as the field separator.
   */
  def apply(path: String, schema: StructType): CsvFileInput = apply(path, ",", schema)

  /**
   * Parses a string and returns a value consistent with the specified data type.
   * If the value cannot be parsed and <tt>nullable</tt> flag is <code>true</code>,
   * it returns <code>null</null>, otherwise it throws a runtime exception.
   */
  def convert(str: String, dataType: DataType, nullable: Boolean) = Try {
    valueOf(str, dataType)
  } recover {
    case NonFatal(e) if nullable => null
    case NonFatal(e) => throw e
  } get
}