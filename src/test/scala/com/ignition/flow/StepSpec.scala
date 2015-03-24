package com.ignition.flow

import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.types.StructType
import org.junit.runner.RunWith
import org.scalacheck.{ Arbitrary, Prop }
import org.specs2.ScalaCheck
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import com.ignition.SparkTestHelper
import com.ignition.types.{ fieldToStruct, int }

@RunWith(classOf[JUnitRunner])
class StepSpec extends Specification with ScalaCheck with SparkTestHelper {

  val schema = int("a").schema

  val dfIntGen = Arbitrary.arbitrary[Int] map { x =>
    val rdd = ctx.sparkContext.parallelize(Seq(Row(x)))
    ctx.createDataFrame(rdd, schema)
  }

  def throwRT() = throw new RuntimeException("runtime")
  def throwWF() = throw FlowExecutionException("workflow")

  abstract class ProducerAdapter extends Producer {
    protected def computeSchema(implicit ctx: SQLContext) = Some(schema)
  }
  abstract class TransformerAdapter extends Transformer {
    protected def computeSchema(inSchema: Option[StructType])(implicit ctx: SQLContext) = Some(schema)
  }
  abstract class SplitterAdapter extends Splitter(2) {
    protected def computeSchema(inSchema: Option[StructType], index: Int)(implicit ctx: SQLContext) = Some(schema)
  }

  abstract class MergerAdapter extends Merger(2) {
    protected def computeSchema(inSchemas: Array[Option[StructType]])(implicit ctx: SQLContext) = Some(schema)
  }

  private def createDF(value: Int) = {
    val schema = int("a").schema
    val rdd = ctx.sparkContext.parallelize(Seq(Row(value)))
    ctx.createDataFrame(rdd, schema)
  }

  "Producer" should {
    "yield output and schema" in Prop.forAll(dfIntGen) { df =>
      val step = new ProducerAdapter { def compute(implicit ctx: SQLContext) = df }
      step.output === df
      step.outputSchema === Some(schema)
    }
    "fail for output(!=0)" in Prop.forAll(dfIntGen) { df =>
      val step = new ProducerAdapter { def compute(implicit ctx: SQLContext) = df }
      step.output(2) must throwA[FlowExecutionException]
    }
    "wrap runtime error into workflow exception" in {
      val step = new ProducerAdapter { def compute(implicit ctx: SQLContext) = throwRT }
      step.output must throwA[FlowExecutionException](message = "Step computation failed")
    }
    "propagate workflow exception" in {
      val step = new ProducerAdapter { def compute(implicit ctx: SQLContext) = throwWF }
      step.output must throwA[FlowExecutionException](message = "workflow")
    }
  }

  "Transformer" should {
    "yield output" in Prop.forAll(dfIntGen) { df =>
      val step0 = new ProducerAdapter { def compute(implicit ctx: SQLContext) = df }
      val step1 = new TransformerAdapter { def compute(arg: DataFrame)(implicit ctx: SQLContext) = arg }
      step0 --> step1
      step1.output === df
    }
    "fail for output(!=0)" in Prop.forAll(dfIntGen) { df =>
      val step0 = new ProducerAdapter { def compute(implicit ctx: SQLContext) = df }
      val step1 = new TransformerAdapter { def compute(arg: DataFrame)(implicit ctx: SQLContext) = arg }
      step0 --> step1
      step1.output(2) must throwA[FlowExecutionException]
    }
    "throw exception when not connected" in Prop.forAll(dfIntGen) { df =>
      val step1 = new TransformerAdapter { def compute(arg: DataFrame)(implicit ctx: SQLContext) = arg }
      step1.output must throwA[FlowExecutionException](message = "Input0 is not connected")
    }
  }

  "Splitter" should {
    "yield outputs 1&2" in Prop.forAll(dfIntGen) { df =>
      val step0 = new ProducerAdapter { def compute(implicit ctx: SQLContext) = df }
      val step1 = new SplitterAdapter {
        protected def compute(arg: DataFrame, index: Int)(implicit ctx: SQLContext) = arg
      }
      step0 --> step1
      step1.output(0) === df
      step1.output(1) === df
    }
    "fail for output(>=2)" in Prop.forAll(dfIntGen) { df =>
      val step0 = new ProducerAdapter { def compute(implicit ctx: SQLContext) = df }
      val step1 = new SplitterAdapter {
        protected def compute(arg: DataFrame, index: Int)(implicit ctx: SQLContext) = arg
      }
      step0 --> step1
      step1.output(2) must throwA[FlowExecutionException]
    }
    "throw exception when not connected" in Prop.forAll(dfIntGen) { df =>
      val step1 = new SplitterAdapter {
        protected def compute(arg: DataFrame, index: Int)(implicit ctx: SQLContext) = arg
      }
      step1.output(0) must throwA[FlowExecutionException](message = "Input0 is not connected")
    }
  }

  "Merger" should {
    "yield output" in Prop.forAll(dfIntGen) { df =>
      val step0 = new ProducerAdapter { def compute(implicit ctx: SQLContext) = df }
      val step1 = new ProducerAdapter { def compute(implicit ctx: SQLContext) = df }
      val step2 = new MergerAdapter {
        protected def compute(args: Array[DataFrame])(implicit ctx: SQLContext): DataFrame = args(0)
      }
      (step0, step1) --> step2
      step2.output === df
    }
    "fail for output(!=0)" in Prop.forAll(dfIntGen) { df =>
      val step0 = new ProducerAdapter { def compute(implicit ctx: SQLContext) = df }
      val step1 = new ProducerAdapter { def compute(implicit ctx: SQLContext) = df }
      val step2 = new MergerAdapter {
        protected def compute(args: Array[DataFrame])(implicit ctx: SQLContext): DataFrame = args(0)
      }
      (step0, step1) --> step2
      step2.output(1) must throwA[FlowExecutionException]
    }
    "throw exception when not connected" in Prop.forAll(dfIntGen) { df =>
      val step2 = new MergerAdapter {
        protected def compute(args: Array[DataFrame])(implicit ctx: SQLContext): DataFrame = args(0)
      }
      step2.output must throwA[FlowExecutionException](message = "Input0 is not connected")
    }
  }
}