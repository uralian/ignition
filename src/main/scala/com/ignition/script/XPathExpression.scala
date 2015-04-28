package com.ignition.script

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import scala.xml.XML

/**
 * XPath expression row processor.
 *
 * @author Vlad Orzhekhovskiy
 */
case class XPathExpression(val srcField: String, val query: String) extends RowExpression[StringType] {

  val targetType = Some(StringType)
  
  def evaluate(schema: StructType)(row: Row) = {
    val index = schema.fieldNames.indexOf(srcField)
    val xml = XML.loadString(row.getString(index))
    (xml \\ query) toString
  }
}