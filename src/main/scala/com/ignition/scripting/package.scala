package com.ignition

import collection.JavaConverters._
import com.ignition.data._
import com.ignition.data.DataType._

/**
 * Scripting implicits and aliases.
 *
 * @author Vlad Orzhekhovskiy
 */
package object scripting {

  /**
   * Converts DataRow into a Java Map[String, Any], for use by Java-based
   * expression evaluators, like MVEL.
   */
  implicit def row2map(row: DataRow): java.util.Map[String, Any] = {
    val javaData = row.rawData map {
      case x: BigDecimal => x.bigDecimal
      case x => x
    }
    (row.columnNames zip javaData toMap).asJava
  }

  implicit class RichString(val expr: String) extends AnyVal {
    def json(src: String) = new JsonRowExProcessor(src, expr)
    def xpath(src: String) = new XPathRowExProcessor(src, expr)
    def mvel[T](implicit dt: DataType[T]) = new MvelRowExProcessor[T](expr)(dt)
  }
}