package com.ignition

import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.DStream

/**
 * Data types, implicits, aliases for DStream-based workflows.
 *
 * @author Vlad Orzhekhovskiy
 */
package object stream {
  type DataStream = DStream[Row]
}