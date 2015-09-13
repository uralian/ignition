package org.apache.spark

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.catalyst.expressions.In

/**
 * @author Vlad Orzhekhovskiy
 */
package object sql {

  /**
   * An extension of Spark SQL Column to provide some useful methods.
   */
  implicit class RichColumn(val self: Column) extends AnyVal {
    
    def IN(values: Any*): Column = {
      val list = values map {
        case c: Column => c.expr
        case x @ _ => lit(x).expr
      }
      new Column(In(self.expr, list))
    }
  }

}