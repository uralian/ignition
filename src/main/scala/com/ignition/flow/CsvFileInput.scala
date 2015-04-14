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

  protected def compute(implicit ctx: SQLContext): DataFrame = {
    val schema = this.schema
    val separator = this.separator

    val rdd = ctx.sparkContext.textFile(path) map { line =>
      val arr = line.split(separator) zip schema map {
        case (str, field) => convert(str, field.dataType, field.nullable)
      }
      Row.fromSeq(arr)
    }
    ctx.createDataFrame(rdd, schema)
  }

  protected def computeSchema(implicit ctx: SQLContext) = Some(schema)

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}

/**
 * CSV Input companion object.
 */
object CsvFileInput {
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