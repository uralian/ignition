package com.ignition.frame

import scala.io.Source
import scala.util.matching.Regex

import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types.{ DataType, StructType }

import com.ignition.SparkRuntime
import com.ignition.types._

/**
 * Reads the text file into a data frame with a single column.
 * If the separator is specified, splits the file into multiple rows, otherwise
 * the data frame will contain only one row.
 *
 * @author Vlad Orzhekhovskiy
 */
case class TextFileInput(path: String, separator: Option[Regex] = None, dataField: String = "content") extends FrameProducer {

  val schema: StructType = string(dataField)

  protected def compute(limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
    val path = injectGlobals(this.path)

    val src = Source.fromFile(path)
    val lines = try src.getLines.mkString("\n") finally src.close

    val fragments = separator map (_.split(lines)) getOrElse Array(lines)
    val rdd = sc.parallelize(fragments map (Row(_)))

    val df = ctx.createDataFrame(rdd, schema)
    optLimit(df, limit)
  }

  protected def computeSchema(implicit runtime: SparkRuntime): StructType = schema
}