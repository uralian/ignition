package com.ignition.frame

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import com.ignition.SparkRuntime

/**
 * Reads a JSON file, which contains a separate JSON object in each line.
 *
 * @author Vlad Orzhekhovskiy
 */
case class JsonFileInput(path: String, columns: Iterable[(String, String)])
  extends FrameProducer {

  def path(p: String): JsonFileInput = copy(path = p)
  def columns(c: Iterable[(String, String)]): JsonFileInput = copy(columns = this.columns ++ c)

  protected def compute(limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
    val path = (injectEnvironment _ andThen injectVariables)(this.path)

    val df = ctx.jsonFile(path)
    val cols = columns map {
      case (name, path) => df.col(path).as(name)
    }

    val result = df.select(cols.toSeq: _*)
    optLimit(result, limit)
  }

  protected def computeSchema(implicit runtime: SparkRuntime): StructType =
    computedSchema(0)
}

/**
 * JSON file input companion object.
 */
object JsonFileInput {
  def apply(path: String, columns: (String, String)*): JsonFileInput = apply(path, columns.toSeq)
}