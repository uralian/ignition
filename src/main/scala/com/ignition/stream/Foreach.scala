package com.ignition.stream

import scala.xml.{ Elem, Node }

import org.apache.spark.sql.DataFrame
import org.json4s.JValue
import org.json4s.JsonDSL.{ pair2Assoc, string2jvalue }

import com.ignition.{ ExecutionException, SparkRuntime, Splitter, Transformer }
import com.ignition.frame.{ FrameProducer, FrameSubSplitter, StepFactory }

/**
 * Invokes a DataFrame SubFlow on each stream batch.
 * The flow passed in the constructor should expect the RDDs from
 * the stream to appear on the first input (index 0).
 *
 * @author Vlad Orzhekhovskiy
 */
case class Foreach(flow: Splitter[DataFrame]) extends StreamSplitter(flow.outputCount) {
  import Foreach._

  protected def compute(arg: DataStream, index: Int, preview: Boolean)(implicit runtime: SparkRuntime): DataStream = {
    val flow = this.flow

    arg transform { rdd =>
      if (rdd.isEmpty) rdd
      else {
        val schema = rdd.first.schema
        val df = runtime.ctx.createDataFrame(rdd, schema)
        val source = new FrameProducer { self =>
          protected def compute(preview: Boolean)(implicit runtime: SparkRuntime) = optLimit(df, preview)
          def toXml: scala.xml.Elem = ???
          def toJson: org.json4s.JValue = ???
        }
        source --> flow
        flow.output(index, preview).rdd
      }
    }
  }

  def toXml: Elem = <node>{ flow.toXml }</node>.copy(label = tag)
  def toJson: JValue = ("tag" -> tag) ~ ("flow" -> flow.toJson)
}

/**
 * Transform companion object.
 */
object Foreach {
  val tag = "stream-foreach"

  def apply(tx: Transformer[DataFrame]): Foreach = {
    val flow = FrameSubSplitter {
      (tx, Seq(tx))
    }
    Foreach(flow)
  }

  def fromXml(xml: Node) = {
    val flow = StepFactory.fromXml(scala.xml.Utility.trim(xml).child.head)
    flow match {
      case f: Transformer[DataFrame] => apply(f)
      case f: Splitter[DataFrame] => apply(f)
    }
  }

  def fromJson(json: JValue) = {
    val flow = StepFactory.fromJson(json \ "flow")
    flow match {
      case f: Transformer[DataFrame] => apply(f)
      case f: Splitter[DataFrame] => apply(f)
    }
  }
}