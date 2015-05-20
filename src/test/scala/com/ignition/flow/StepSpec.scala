package com.ignition.flow

import org.apache.spark.annotation.{ DeveloperApi, Experimental }
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.types.StructType
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.scalacheck.{ Arbitrary, Prop }
import org.specs2.ScalaCheck

import com.ignition.SparkTestHelper
import com.ignition.types._
import com.ignition.SparkRuntime

@RunWith(classOf[JUnitRunner])
class StepSpec extends FlowSpecification with ScalaCheck with SparkTestHelper {
  import ctx.implicits._

  sequential

  val schema = int("a").schema

  val dfIntGen = Arbitrary.arbitrary[Int] map { x =>
    val rdd = ctx.sparkContext.parallelize(Seq(Row(x), Row(x), Row(x)))
    ctx.createDataFrame(rdd, schema)
  }

  def throwRT() = throw new RuntimeException("runtime")
  def throwWF() = throw FlowExecutionException("workflow")

  abstract class ProducerAdapter extends Producer {
    protected def computeSchema(implicit runtime: SparkRuntime) = schema
  }

  abstract class TransformerAdapter extends Transformer {
    protected def computeSchema(inSchema: StructType)(implicit runtime: SparkRuntime) = schema
  }

  abstract class SplitterAdapter extends Splitter(2) {
    protected def computeSchema(inSchema: StructType, index: Int)(implicit runtime: SparkRuntime) = schema
  }

  abstract class MergerAdapter extends Merger(2) {
    protected def computeSchema(inSchemas: Array[StructType])(implicit runtime: SparkRuntime) = schema
  }

  abstract class ModuleAdapter extends Module(2, 3) {
    protected def computeSchema(inSchemas: Array[StructType], index: Int)(implicit runtime: SparkRuntime) = schema
  }

  private def createDF(value: Int) = {
    val schema = int("a").schema
    val rdd = ctx.sparkContext.parallelize(Seq(Row(value)))
    ctx.createDataFrame(rdd, schema)
  }

  "Producer" should {
    "yield output and schema" in Prop.forAll(dfIntGen) { df =>
      val step = new ProducerAdapter {
        def compute(limit: Option[Int])(implicit runtime: SparkRuntime) = limit map df.limit getOrElse df
      }
      step.output === df
      step.output(Some(2)).collect === df.limit(2).collect
      step.outSchema === schema
    }
    "fail for output(!=0)" in Prop.forAll(dfIntGen) { df =>
      val step = new ProducerAdapter { def compute(limit: Option[Int])(implicit runtime: SparkRuntime) = df }
      step.output(2) must throwA[FlowExecutionException]
    }
    "wrap runtime error into workflow exception" in {
      val step = new ProducerAdapter { def compute(limit: Option[Int])(implicit runtime: SparkRuntime) = throwRT }
      step.output must throwA[FlowExecutionException](message = "Step computation failed")
    }
    "propagate workflow exception" in {
      val step = new ProducerAdapter { def compute(limit: Option[Int])(implicit runtime: SparkRuntime) = throwWF }
      step.output must throwA[FlowExecutionException](message = "workflow")
    }
  }

  "Transformer" should {
    "yield output" in Prop.forAll(dfIntGen) { df =>
      val step0 = new ProducerAdapter { def compute(limit: Option[Int])(implicit runtime: SparkRuntime) = df }
      val step1 = new TransformerAdapter {
        def compute(arg: DataFrame, limit: Option[Int])(implicit runtime: SparkRuntime) =
          limit map arg.limit getOrElse arg
      }
      step0 --> step1
      step1.output === df
      step1.output(Some(1)).collect === df.limit(1).collect
    }
    "fail for output(!=0)" in Prop.forAll(dfIntGen) { df =>
      val step0 = new ProducerAdapter { def compute(limit: Option[Int])(implicit runtime: SparkRuntime) = df }
      val step1 = new TransformerAdapter { def compute(arg: DataFrame, limit: Option[Int])(implicit runtime: SparkRuntime) = arg }
      step0 --> step1
      step1.output(2) must throwA[FlowExecutionException]
    }
    "throw exception when not connected" in Prop.forAll(dfIntGen) { df =>
      val step1 = new TransformerAdapter { def compute(arg: DataFrame, limit: Option[Int])(implicit runtime: SparkRuntime) = arg }
      step1.output must throwA[FlowExecutionException](message = "Input0 is not connected")
    }
  }

  "Splitter" should {
    "yield outputs 1&2" in Prop.forAll(dfIntGen) { df =>
      val step0 = new ProducerAdapter { def compute(limit: Option[Int])(implicit runtime: SparkRuntime) = df }
      val step1 = new SplitterAdapter {
        protected def compute(arg: DataFrame, index: Int, limit: Option[Int])(implicit runtime: SparkRuntime) =
          limit map arg.limit getOrElse arg
      }
      step0 --> step1
      step1.output(0) === df
      step1.output(0, Some(1)).collect === df.limit(1).collect
      step1.output(1) === df
      step1.output(1, Some(2)).collect === df.limit(2).collect
    }
    "fail for output(>=2)" in Prop.forAll(dfIntGen) { df =>
      val step0 = new ProducerAdapter { def compute(limit: Option[Int])(implicit runtime: SparkRuntime) = df }
      val step1 = new SplitterAdapter {
        protected def compute(arg: DataFrame, index: Int, limit: Option[Int])(implicit runtime: SparkRuntime) = arg
      }
      step0 --> step1
      step1.output(2) must throwA[FlowExecutionException]
    }
    "throw exception when not connected" in Prop.forAll(dfIntGen) { df =>
      val step1 = new SplitterAdapter {
        protected def compute(arg: DataFrame, index: Int, limit: Option[Int])(implicit runtime: SparkRuntime) = arg
      }
      step1.output(0) must throwA[FlowExecutionException](message = "Input0 is not connected")
    }
  }

  "Merger" should {
    "yield output" in Prop.forAll(dfIntGen) { df =>
      val step0 = new ProducerAdapter { def compute(limit: Option[Int])(implicit runtime: SparkRuntime) = df }
      val step1 = new ProducerAdapter { def compute(limit: Option[Int])(implicit runtime: SparkRuntime) = df }
      val step2 = new MergerAdapter {
        protected def compute(args: Array[DataFrame], limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame =
          limit map args(0).limit getOrElse args(0)
      }
      (step0, step1) --> step2
      step2.output === df
      step2.output(Some(1)).collect === df.limit(1).collect
    }
    "fail for output(!=0)" in Prop.forAll(dfIntGen) { df =>
      val step0 = new ProducerAdapter { def compute(limit: Option[Int])(implicit runtime: SparkRuntime) = df }
      val step1 = new ProducerAdapter { def compute(limit: Option[Int])(implicit runtime: SparkRuntime) = df }
      val step2 = new MergerAdapter {
        protected def compute(args: Array[DataFrame], limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = args(0)
      }
      (step0, step1) --> step2
      step2.output(1) must throwA[FlowExecutionException]
    }
    "throw exception when not connected" in Prop.forAll(dfIntGen) { df =>
      val step2 = new MergerAdapter {
        protected def compute(args: Array[DataFrame], limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = args(0)
      }
      step2.output must throwA[FlowExecutionException](message = "Input0 is not connected")
    }
  }

  "Module" should {
    "yield output" in prop { (x: Int, y: Int) =>
      val step0 = new ProducerAdapter { def compute(limit: Option[Int])(implicit runtime: SparkRuntime) = createDF(x) }
      val step1 = new ProducerAdapter { def compute(limit: Option[Int])(implicit runtime: SparkRuntime) = createDF(y) }
      val step2 = new ModuleAdapter {
        def compute(args: Array[DataFrame], index: Int, limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = index match {
          case 0 => args(1).select((args(1).col("a") / 2).cast("int").as("y2"))
          case 1 => args(0).select((args(0).col("a") / 4).cast("int").as("x4"))
          case 2 => args(0).unionAll(args(1))
        }
      }
      (step0, step1) --> step2
      assertOutput(step2, 0, Seq((y / 2).toInt))
      step2.outSchema(0) === schema
      assertOutput(step2, 1, Seq((x / 4).toInt))
      step2.outSchema(1) === schema
      assertOutput(step2, 2, Seq(x), Seq(y))
      step2.outSchema(2) === schema
    }
  }

  "Step connection operators" should {
    val p1 = new ProducerAdapter { val name = "p1"; def compute(limit: Option[Int])(implicit runtime: SparkRuntime) = ??? }
    val t1 = new TransformerAdapter { val name = "t1"; def compute(arg: DataFrame, limit: Option[Int])(implicit runtime: SparkRuntime) = ??? }
    val t2 = new TransformerAdapter { val name = "t2"; def compute(arg: DataFrame, limit: Option[Int])(implicit runtime: SparkRuntime) = ??? }
    val t3 = new TransformerAdapter { val name = "t3"; def compute(arg: DataFrame, limit: Option[Int])(implicit runtime: SparkRuntime) = ??? }
    val s2 = new SplitterAdapter { def compute(arg: DataFrame, index: Int, limit: Option[Int])(implicit runtime: SparkRuntime) = ??? }
    val m2 = new MergerAdapter { def compute(args: Array[DataFrame], limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = ??? }
    "connect producers and transformers with `to`" in {
      (p1 to t1 to t2 to t3) === t3
      t1.ins(0) === Tuple2(p1, 0)
      t2.ins(0) === Tuple2(t1, 0)
      t3.ins(0) === Tuple2(t2, 0)
    }
    "connect producers and transformers with `-->`" in {
      (p1 --> t1 --> t2) === t2
      t1.ins(0) === Tuple2(p1, 0)
      t2.ins(0) === Tuple2(t1, 0)
      t3.ins(0) === Tuple2(t2, 0)
    }
    "connect multi-port steps with `out()` and `in()`" in {
      s2.out(0) to m2.in(1)
      m2.ins(1) === Tuple2(s2, 0)
      (s2.out(1) to t1 to t2) === t2
      t1.ins(0) === Tuple2(s2, 1)
      t2.ins(0) === Tuple2(t1, 0)
      p1 to t1 to m2.in(0)
      t1.ins(0) === Tuple2(p1, 0)
      m2.ins(0) === Tuple2(t1, 0)
    }
    "connect multi-port steps with |: and :|" in {
      s2 |: 1 --> 0 :| m2
      m2.ins(0) === Tuple2(s2, 1)
      s2 |: 1 --> t1
      t1.ins(0) === Tuple2(s2, 1)
      p1 --> t1 --> 1 :| m2
      t1.ins(0) === Tuple2(p1, 0)
      m2.ins(1) === Tuple2(t1, 0)
    }
    "connect products with multi-input steps" in {
      (t1, t2) to m2
      m2.ins(0) === Tuple2(t1, 0)
      m2.ins(1) === Tuple2(t2, 0)
      (t1, t2) --> m2
      m2.ins(0) === Tuple2(t1, 0)
      m2.ins(1) === Tuple2(t2, 0)
    }
    "connect muti-output steps with products" in {
      s2 to (t1, t2)
      t1.ins(0) === Tuple2(s2, 0)
      t2.ins(0) === Tuple2(s2, 1)
      s2 --> (t1, t2)
      t1.ins(0) === Tuple2(s2, 0)
      t2.ins(0) === Tuple2(s2, 1)
    }
  }
}