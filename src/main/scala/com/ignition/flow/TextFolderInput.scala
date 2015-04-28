package com.ignition.flow

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.ignition.types._

/**
 * Reads a folder of text files.
 *
 * @author Vlad Orzhekhovskiy
 */
case class TextFolderInput(path: String, nameField: String = "filename",
  dataField: String = "content") extends Producer {

  val schema = string(nameField) ~ string(dataField)

  protected def compute(limit: Option[Int])(implicit ctx: SQLContext): DataFrame = {
    val rdd = ctx.sparkContext.wholeTextFiles(path) map {
      case (fileName, contents) => Row(fileName, contents)
    }
    val df = ctx.createDataFrame(rdd, schema)
    limit map df.limit getOrElse df
  }

  protected def computeSchema(implicit ctx: SQLContext): StructType = schema

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}