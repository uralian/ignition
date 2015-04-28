package com.ignition.script

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import com.fasterxml.jackson.databind.ObjectMapper
import io.gatling.jsonpath.JsonPath

/**
 * JSON path processor, based on https://github.com/gatling/jsonpath implementation.
 *
 * @author Vlad Orzhekhovskiy
 */
case class JsonPathExpression(val srcField: String, val query: String) extends RowExpression[StringType] {

  @transient private lazy val compiled = JsonPath.compile(query)

  private val mapper = new ObjectMapper
  
  val targetType = Some(StringType)

  def evaluate(schema: StructType)(row: Row) = compiled.right.map { path =>
    val index = schema.fieldNames.indexOf(srcField)
    val json = mapper.readValue(row.getString(index), classOf[Object])
    path.query(json) mkString
  } match {
    case Left(err) => throw new RuntimeException(err.reason)
    case Right(str) => str
  }
}