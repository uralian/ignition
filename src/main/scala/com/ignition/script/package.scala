package com.ignition

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import collection.JavaConverters._

/**
 * Scripting implicits and aliases.
 *
 * @author Vlad Orzhekhovskiy
 */
package object script {

  def row2javaMap(schema: StructType)(row: Row): java.util.Map[String, Any] =
    (schema.fieldNames zip row.toSeq toMap) asJava

  implicit class RichString(val expr: String) extends AnyVal {
    def json(src: String) = new JsonPathExpression(src, expr)
    def xpath(src: String) = new XPathExpression(src, expr)
    def mvel() = new MvelExpression(expr)
  }
}