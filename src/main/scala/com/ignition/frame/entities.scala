package com.ignition.frame

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import com.ignition._
import com.ignition.util.ConfigUtils

/**
 * Workflow step that emits DataFrame as the output.
 */
trait FrameStep extends Step[DataFrame, SparkRuntime] with XmlExport with JsonExport {

  /**
   * Returns the implicit SQLContext.
   */
  protected def ctx(implicit runtime: SparkRuntime) = runtime.ctx

  /**
   * Returns the implicit SparkContext.
   */
  protected def sc(implicit runtime: SparkRuntime) = runtime.sc

  /**
   * Returns the output schema of the step at a given index.
   */
  final def outSchema(index: Int)(implicit runtime: SparkRuntime): StructType = wrap {
    assert(0 until outputCount contains index, s"Output index out of range: $index of $outputCount")
    buildSchema(index)
  }

  /**
   * Returns the output schema of the step at index 0.
   */
  final def outSchema(implicit runtime: SparkRuntime): StructType = outSchema(0)

  /**
   * Returns the output schema of the step at a given index by calling output() in preview mode
   * and retrieving the schema from the DataFrame. Since the default implementation of this method
   * calls output(), invoking it from output() directly or indirectly will result in infinite cycle.
   */
  protected def buildSchema(index: Int)(implicit runtime: SparkRuntime): StructType =
    output(index, true).schema

  /**
   * Takes one row from the data frame is preview is true.
   */
  private[ignition] def optLimit(df: DataFrame, preview: Boolean): DataFrame =
    if (preview) df.limit(FrameStep.previewSize) else df
}

/**
 * Constants and helper functions for FrameStep.
 */
object FrameStep {
  import ConfigUtils._

  lazy val previewSize = ConfigUtils.rootConfig.getInt("preview-size")
}

/* step templates */

abstract class FrameProducer extends Producer[DataFrame, SparkRuntime] with FrameStep

abstract class FrameTransformer extends Transformer[DataFrame, SparkRuntime] with FrameStep

abstract class FrameSplitter(val outputCount: Int) extends Splitter[DataFrame, SparkRuntime] with FrameStep

abstract class FrameMerger(val inputCount: Int) extends Merger[DataFrame, SparkRuntime] with FrameStep

abstract class FrameModule(val inputCount: Int, val outputCount: Int) extends Module[DataFrame, SparkRuntime] with FrameStep

/* subflow templates */

trait FrameSubFlow extends FrameStep {
  val tag = FrameSubFlow.tag
}

case class FrameSubProducer(body: ConnectionSource[DataFrame, SparkRuntime])
  extends SubProducer[DataFrame, SparkRuntime](body) with FrameSubFlow

case class FrameSubTransformer(body: (ConnectionTarget[DataFrame, SparkRuntime], ConnectionSource[DataFrame, SparkRuntime]))
  extends SubTransformer[DataFrame, SparkRuntime](body) with FrameSubFlow

case class FrameSubSplitter(body: (ConnectionTarget[DataFrame, SparkRuntime], Seq[ConnectionSource[DataFrame, SparkRuntime]]))
  extends SubSplitter[DataFrame, SparkRuntime](body) with FrameSubFlow

case class FrameSubMerger(body: (Seq[ConnectionTarget[DataFrame, SparkRuntime]], ConnectionSource[DataFrame, SparkRuntime]))
  extends SubMerger[DataFrame, SparkRuntime](body) with FrameSubFlow

case class FrameSubModule(body: (Seq[ConnectionTarget[DataFrame, SparkRuntime]], Seq[ConnectionSource[DataFrame, SparkRuntime]]))
    extends SubModule[DataFrame, SparkRuntime](body) with FrameStep {
  val tag = FrameSubFlow.tag
}

/**
 * Provides SubFlow common methods.
 */
object FrameSubFlow extends SubFlowFactory[FrameStep, DataFrame, SparkRuntime] {

  val tag = "subflow"

  val xmlFactory = FrameStepFactory

  val jsonFactory = FrameStepFactory

  /**
   * Depending on the number of inputs and outputs, the returned instance can be a
   * FrameSubProducer, FrameSubTransformer, FrameSubSplitter,
   * FrameSubMerger, or FrameSubModule.
   */
  def instantiate(inPoints: Seq[ConnectionTarget[DataFrame, SparkRuntime]],
                  outPoints: Seq[ConnectionSource[DataFrame, SparkRuntime]]): FrameStep with SubFlow[DataFrame, SparkRuntime] =
    (inPoints.size, outPoints.size) match {
      case (0, 1)          => FrameSubProducer(outPoints(0))
      case (1, 1)          => FrameSubTransformer((inPoints(0), outPoints(0)))
      case (1, o) if o > 1 => FrameSubSplitter((inPoints(0), outPoints))
      case (i, 1) if i > 1 => FrameSubMerger((inPoints, outPoints(0)))
      case _               => FrameSubModule((inPoints, outPoints))
    }
}

/**
 * Variable literal.
 */
case class VarLiteral(name: String) {
  def eval(implicit runtime: SparkRuntime) = runtime.vars(name)
  def evalAs[T](implicit runtime: SparkRuntime) = eval.asInstanceOf[T]
}

/**
 * Environment literal.
 */
case class EnvLiteral(name: String) {
  def eval = System.getProperty(name)
}