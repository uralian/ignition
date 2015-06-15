package com.ignition.stream

import org.apache.spark.sql.types.StructType
import com.ignition.SparkRuntime
import com.ignition.frame.SubFlow
import com.ignition.frame.DataGrid
import com.ignition.frame.FrameTransformer
import com.ignition.frame.FrameProducer
import org.apache.spark.sql.DataFrame
import com.ignition.frame.FlowInput
import com.ignition.frame.FrameSplitter
import com.ignition.Splitter

/**
 * Invokes a DataFrame SubFlow on each stream batch.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Transform(flow: SubFlow) extends StreamSplitter(flow.outputCount) {

  protected def compute(arg: DataStream, index: Int, limit: Option[Int])(implicit runtime: SparkRuntime): DataStream = {
    val flow = this.flow

    arg transform { rdd =>
      if (rdd.isEmpty) rdd
      else {
        val schema = rdd.first.schema
        val df = runtime.ctx.createDataFrame(rdd, schema)
        val source = new FrameProducer {
          protected def compute(limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
            optLimit(df, limit)
          }
          override protected def computeSchema(implicit runtime: SparkRuntime): StructType = schema
        }
        source --> flow
        flow.output(index).rdd
      }
    }
  }

  protected def computeSchema(inSchema: StructType, index: Int)(implicit runtime: SparkRuntime): StructType =
    computedSchema(index)
}

/**
 * Transform companion object.
 */
object Transform {

  def apply(tx: FrameTransformer): Transform = {
    val flow = SubFlow(1, 1) { (input, output) =>
      input --> tx --> output
    }
    Transform(flow)
  }

  def apply(split: FrameSplitter): Transform = {
    val flow = SubFlow(1, split.outputCount) { (input, output) =>
      input --> split
      (0 until split.outputCount) foreach { n =>
        split.out(n) --> output.in(n)
      }
    }
    Transform(flow)
  }
}