package com.ignition.frame

import scala.annotation.elidable
import scala.annotation.elidable.ASSERTION
import scala.xml.Node

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.json4s.{ JValue, jvalue2monadic }

import com.ignition._
import com.ignition.util.ConfigUtils
import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.RichNodeSeq

/**
 * Workflow step that emits DataFrame as the output.
 */
trait FrameStep extends Step[DataFrame] with XmlExport with JsonExport {

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

abstract class FrameProducer extends Producer[DataFrame] with FrameStep

abstract class FrameTransformer extends Transformer[DataFrame] with FrameStep

abstract class FrameSplitter(val outputCount: Int) extends Splitter[DataFrame] with FrameStep

abstract class FrameMerger(val inputCount: Int) extends Merger[DataFrame] with FrameStep

abstract class FrameModule(val inputCount: Int, val outputCount: Int) extends Module[DataFrame] with FrameStep

/* subflow templates */

case class FrameSubProducer(body: ConnectionSource[DataFrame])
  extends SubProducer[DataFrame](body) with FrameStep

case class FrameSubTransformer(body: (ConnectionTarget[DataFrame], ConnectionSource[DataFrame]))
  extends SubTransformer[DataFrame](body) with FrameStep

case class FrameSubSplitter(body: (ConnectionTarget[DataFrame], Seq[ConnectionSource[DataFrame]]))
  extends SubSplitter[DataFrame](body) with FrameStep

case class FrameSubMerger(body: (Seq[ConnectionTarget[DataFrame]], ConnectionSource[DataFrame]))
  extends SubMerger[DataFrame](body) with FrameStep

case class FrameSubModule(body: (Seq[ConnectionTarget[DataFrame]], Seq[ConnectionSource[DataFrame]]))
  extends SubModule[DataFrame](body) with FrameStep

/**
 * Provides SubFlow common methods.
 */
object FrameSubFlow {

  /**
   * Restores a subflow from the XML node. Depending on the number of inputs and outputs,
   * the returned instance can be a FrameSubProducer, FrameSubTransformer, FrameSubSplitter,
   * FrameSubMerger, or FrameSubModule.
   */
  def fromXml(xml: Node) = {
    val stepById = (xml \ "steps" \ "_") map { node =>
      val id = node \ "@id" asString
      val step = StepFactory.fromXml(node)
      id -> step
    } toMap

    xml \ "connections" \ "connect" foreach { n =>
      val srcStep = stepById(n \ "@src" asString)
      val srcPort = n \ "@srcPort" asInt
      val tgtStep = stepById(n \ "@tgt" asString)
      val tgtPort = n \ "@tgtPort" asInt

      outs(srcStep)(srcPort) --> ins(tgtStep)(tgtPort)
    }

    val inPoints = xml \ "in-points" \ "step" map { n =>
      val srcStep = stepById(n \ "@id" asString)
      val srcPort = n \ "@port" asInt

      ins(srcStep)(srcPort)
    }

    val outPoints = xml \ "out-points" \ "step" map { n =>
      val tgtStep = stepById(n \ "@id" asString)
      val tgtPort = n \ "@port" asInt

      outs(tgtStep)(tgtPort)
    }

    createSubflow(inPoints, outPoints)
  }

  /**
   * Restores a subflow from the JSON node. Depending on the number of inputs and outputs,
   * the returned instance can be a FrameSubProducer, FrameSubTransformer, FrameSubSplitter,
   * FrameSubMerger, or FrameSubModule.
   */
  def fromJson(json: JValue) = {
    val stepById = (json \ "steps" asArray) map { node =>
      val id = node \ "id" asString
      val step = StepFactory.fromJson(node)
      id -> step
    } toMap

    (json \ "connections" asArray) foreach { n =>
      val srcStep = stepById(n \ "src" asString)
      val srcPort = n \ "srcPort" asInt
      val tgtStep = stepById(n \ "tgt" asString)
      val tgtPort = n \ "tgtPort" asInt

      outs(srcStep)(srcPort) --> ins(tgtStep)(tgtPort)
    }

    val inPoints = (json \ "in-points" asArray) map { n =>
      val srcStep = stepById(n \ "id" asString)
      val srcPort = n \ "port" asInt

      ins(srcStep)(srcPort)
    }

    val outPoints = (json \ "out-points" asArray) map { n =>
      val tgtStep = stepById(n \ "id" asString)
      val tgtPort = n \ "port" asInt

      outs(tgtStep)(tgtPort)
    }

    createSubflow(inPoints, outPoints)
  }

  private def createSubflow(inPoints: Seq[ConnectionTarget[DataFrame]],
                            outPoints: Seq[ConnectionSource[DataFrame]]): FrameStep with SubFlow[DataFrame] =
    (inPoints.size, outPoints.size) match {
      case (0, 1) => FrameSubProducer(outPoints(0))
      case (1, 1) => FrameSubTransformer((inPoints(0), outPoints(0)))
      case (1, o) if o > 1 => FrameSubSplitter((inPoints(0), outPoints))
      case (i, 1) if i > 1 => FrameSubMerger((inPoints, outPoints(0)))
      case _ => FrameSubModule((inPoints, outPoints))
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