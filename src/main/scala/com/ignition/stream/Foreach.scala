package com.ignition.stream

import scala.xml.{ Elem, Node }

import org.apache.spark.sql.DataFrame
import org.json4s.JValue
import org.json4s.JsonDSL.{ pair2Assoc, string2jvalue }
import org.json4s.jvalue2monadic

import com.ignition.{ ExecutionException, Step }
import com.ignition.frame.{ FrameProducer, FrameStepFactory, SparkRuntime }
import com.ignition.ins

/**
 * Invokes the embedded flow on each batch. Passes the input to the 0th input port
 * of the flow's 0th input, and each output of the flow will be directed to the
 * corresponding output of the step.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Foreach(flow: Step[DataFrame, SparkRuntime])
    extends StreamSplitter(flow.outputCount) {
  import Foreach._

  protected def compute(arg: DataStream, index: Int)(implicit runtime: SparkStreamingRuntime): DataStream = {
    val flow = this.flow
    arg transform { rdd =>
      flow.resetCache(true, false)
      if (rdd.isEmpty) rdd
      else {
        val schema = rdd.first.schema
        val df = runtime.ctx.createDataFrame(rdd, schema)
        val source = new FrameProducer { self =>
          protected def compute(implicit runtime: SparkRuntime) = optLimit(df, runtime.previewMode)
          def toXml: scala.xml.Elem = ???
          def toJson: org.json4s.JValue = ???
        }
        source --> ins(flow)(0)
        flow.output(index).rdd
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

  def fromXml(xml: Node) = {
    val flow = FrameStepFactory.fromXml(scala.xml.Utility.trim(xml).child.head)
    apply(flow)
  }

  def fromJson(json: JValue) = {
    val flow = FrameStepFactory.fromJson(json \ "flow")
    apply(flow)
  }
}