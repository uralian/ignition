package com.ignition.frame

import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types.StructType

import com.ignition.SparkRuntime
import com.ignition.types.{ fieldToRichStruct, string }

/**
 * Reads a folder of text files.
 *
 * @author Vlad Orzhekhovskiy
 */
case class TextFolderInput(path: String, nameField: String = "filename",
  dataField: String = "content") extends FrameProducer {

  def nameField(field: String): TextFolderInput = copy(nameField = field)
  def dataField(field: String): TextFolderInput = copy(dataField = field)

  val schema = string(nameField) ~ string(dataField)

  protected def compute(limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
    val path = (injectEnvironment _ andThen injectVariables)(this.path)

    val rdd = ctx.sparkContext.wholeTextFiles(path) map {
      case (fileName, contents) => Row(fileName, contents)
    }
    val df = ctx.createDataFrame(rdd, schema)
    optLimit(df, limit)
  }

  protected def computeSchema(implicit runtime: SparkRuntime): StructType = schema
}