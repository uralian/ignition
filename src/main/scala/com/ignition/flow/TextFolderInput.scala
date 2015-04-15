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

  protected def compute(implicit ctx: SQLContext): DataFrame = {
    val rdd = ctx.sparkContext.wholeTextFiles(path) map {
      case (fileName, contents) => Row(fileName, contents)
    }
    ctx.createDataFrame(rdd, schema)
  }

  protected def computeSchema(implicit ctx: SQLContext) = Some(schema)

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}