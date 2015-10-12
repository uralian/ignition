package com.ignition.stream

//import org.apache.spark.sql.types.StructType
//import com.ignition.SparkRuntime
//import com.ignition.frame.SubFlow
//import com.ignition.frame.DataGrid
//import com.ignition.frame.FrameTransformer
//import com.ignition.frame.FrameProducer
//import org.apache.spark.sql.DataFrame
//import com.ignition.frame.SubFlow.FlowInput
//import com.ignition.frame.FrameSplitter
//import com.ignition.Splitter
//
///**
// * Invokes a DataFrame SubFlow on each stream batch.
// * The flow passed in the constructor should expect the RDDs from
// * the stream to appear on the first input (index 0).
// *
// * @author Vlad Orzhekhovskiy
// */
//case class Foreach(flow: SubFlow) extends StreamSplitter(flow.outputCount) {
//
//  protected def compute(arg: DataStream, index: Int, limit: Option[Int])(implicit runtime: SparkRuntime): DataStream = {
//    val flow = this.flow
//
//    arg transform { rdd =>
//      if (rdd.isEmpty) rdd
//      else {
//        val schema = rdd.first.schema
//        val df = runtime.ctx.createDataFrame(rdd, schema)
//        val source = new FrameProducer {
//          protected def compute(limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
//            optLimit(df, limit)
//          }
//          override protected def computeSchema(implicit runtime: SparkRuntime): StructType = schema
//          def toXml: scala.xml.Elem = ???
//          def toJson: org.json4s.JValue = ???
//        }
//        source --> flow
//        flow.output(index).rdd
//      }
//    }
//  }
//
//  protected def computeSchema(inSchema: StructType, index: Int)(implicit runtime: SparkRuntime): StructType =
//    computedSchema(index)
//
//  def toXml: scala.xml.Elem = ???
//  def toJson: org.json4s.JValue = ???
//}
//
///**
// * Transform companion object.
// */
//object Foreach {
//
//  def apply(tx: FrameTransformer): Foreach = {
//    val flow = SubFlow(1, 1) { (input, output) =>
//      input --> tx --> output
//    }
//    Foreach(flow)
//  }
//
//  def apply(split: FrameSplitter): Foreach = {
//    val flow = SubFlow(1, split.outputCount) { (input, output) =>
//      input --> split
//      (0 until split.outputCount) foreach { n =>
//        split.out(n) --> output.in(n)
//      }
//    }
//    Foreach(flow)
//  }
//}