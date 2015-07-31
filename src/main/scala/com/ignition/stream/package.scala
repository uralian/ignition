package com.ignition

import org.apache.spark.sql.Row
import org.apache.spark.streaming.dstream.DStream

import com.ignition.frame.{ FrameSplitter, FrameTransformer, SubFlow }
import com.ignition.stream.Foreach

/**
 * Data types, implicits, aliases for DStream-based workflows.
 *
 * @author Vlad Orzhekhovskiy
 */
package object stream {
  type DataStream = DStream[Row]

  def foreach(flow: SubFlow): Foreach = Foreach(flow)
  def foreach(tx: FrameTransformer): Foreach = Foreach(tx)
  def foreach(sp: FrameSplitter): Foreach = Foreach(sp)
}