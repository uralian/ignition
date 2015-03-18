package com.ignition.flow

import scala.util.control.NonFatal
import scala.xml.Elem

import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.sql.types.StructType

/**
 * A workflow step. It can have an arbitrary number of inputs and outputs.
 */
sealed trait Step {

  /**
   * The number of output ports.
   */
  def outputCount: Int

  /**
   * The maximum number of input ports.
   */
  def inputCount: Int

  /**
   * Computes a step output value at the specified index.
   */
  @throws(classOf[FlowExecutionException])
  def output(index: Int)(implicit ctx: SQLContext): DataFrame

  /**
   * Returns the output schema of the step or None, if not connected.
   */
  def outputSchema: Option[StructType]
}

/**
 * XML serialization.
 */
trait XmlExport {
  def toXml: Elem
}

/**
 * An abstract implementation base class for Step trait.
 * The following members need to be implemented by subclasses:
 * +outputSchema: Option[StructType]
 * +compute(args: Array[DataFrame], index: Int)(implicit ctx: SQLContext): DataFrame
 */
abstract class AbstractStep(val inputCount: Int, val outputCount: Int) extends Step {
  protected val ins = Array.ofDim[(Step, Int)](inputCount)

  /**
   * Connects an input port to an output port of another step.
   */
  def connectFrom(inIndex: Int, step: Step, outIndex: Int): AbstractStep = {
    assert(0 until step.outputCount contains outIndex, s"Output index out of range: $outIndex")
    ins(inIndex) = (step, outIndex)
    this
  }

  /**
   * Returns the output value at a given index.
   */
  def output(index: Int)(implicit ctx: SQLContext): DataFrame = wrap {
    assert(0 until outputCount contains index, s"Output index out of range: $index")
    compute(inputs, index)
  }

  /**
   * Scans the input ports and retrieves the output values of the connectes steps.
   * If an input is not connected, it throws an error.
   */
  protected def inputs(implicit ctx: SQLContext): Array[DataFrame] = (ins zipWithIndex) map {
    case ((step, index), i) => step.output(index)(ctx)
    case (_, i) => throw FlowExecutionException(s"Input$i is not connected")
  }

  /**
   * Computes the output port with the specified index.
   */
  protected def compute(args: Array[DataFrame], index: Int)(implicit ctx: SQLContext): DataFrame

  /**
   * Wraps exceptions into FlowExecutionException instances.
   */
  protected def wrap[T](body: => T): T = try { body } catch {
    case e: FlowExecutionException => throw e
    case NonFatal(e) => throw FlowExecutionException("Step computation failed", e)
  }

  /**
   * Serialization helper. Used by subclasses in writeObject() method to explicitly
   * prohibit serialization.
   */
  protected def unserializable = throw new java.io.IOException("Object should not be serialized")
}

/**
 * Functions for a step with a single output.
 */
trait SingleOutput { self: AbstractStep =>
  def to(step: Transformer) = { step.connectFrom(0, this, 0); step }
  def ->(step: Transformer) = to(step)

  def to(step: Splitter) = { step.connectFrom(0, this, 0); step }
  def ->(step: Splitter) = to(step)
  
  def output(implicit ctx: SQLContext): DataFrame = output(0)(ctx)
}

/**
 * A step that has one output and no inputs.
 * The following members need to be implemented by subclasses:
 * +outputSchema: Option[StructType]
 * +compute(implicit ctx: SQLContext): DataFrame
 */
abstract class Producer extends AbstractStep(0, 1) with SingleOutput {

  protected def compute(args: Array[DataFrame], index: Int)(implicit ctx: SQLContext): DataFrame = compute(ctx)

  protected def compute(implicit ctx: SQLContext): DataFrame
}

/**
 * A step that has one input and one output.
 * The following members need to be implemented by subclasses:
 * +outputSchema: Option[StructType]
 * +compute(arg: DataFrame)(implicit ctx: SQLContext): DataFrame
 */
abstract class Transformer extends AbstractStep(1, 1) with SingleOutput {

  protected def compute(args: Array[DataFrame], index: Int)(implicit ctx: SQLContext): DataFrame = compute(args(0))(ctx)

  protected def compute(arg: DataFrame)(implicit ctx: SQLContext): DataFrame
}

/**
 * A step that has many outputs and one input.
 * The following members need to be implemented by subclasses:
 * +compute(arg: DataFrame, index: Int)(implicit ctx: SQLContext): DataFrame
 */
abstract class Splitter(override val outputCount: Int) extends AbstractStep(1, outputCount) {

  def _0_>(step: Transformer) = connectTo(0, step)
  def _1_>(step: Transformer) = connectTo(1, step)
  def _2_>(step: Transformer) = connectTo(2, step)
  def _3_>(step: Transformer) = connectTo(3, step)
  def _4_>(step: Transformer) = connectTo(4, step)
  def _5_>(step: Transformer) = connectTo(5, step)
  def _6_>(step: Transformer) = connectTo(6, step)
  def _7_>(step: Transformer) = connectTo(7, step)
  def _8_>(step: Transformer) = connectTo(8, step)
  def _9_>(step: Transformer) = connectTo(9, step)

  def _0_>(step: Splitter) = connectTo(0, step)
  def _1_>(step: Splitter) = connectTo(1, step)
  def _2_>(step: Splitter) = connectTo(2, step)
  def _3_>(step: Splitter) = connectTo(3, step)
  def _4_>(step: Splitter) = connectTo(4, step)
  def _5_>(step: Splitter) = connectTo(5, step)
  def _6_>(step: Splitter) = connectTo(6, step)
  def _7_>(step: Splitter) = connectTo(7, step)
  def _8_>(step: Splitter) = connectTo(8, step)
  def _9_>(step: Splitter) = connectTo(9, step)

  def to(product: Product) = (product.productIterator.zipWithIndex) foreach {
    case (step: Transformer, index) => connectTo(index, step)
    case (step: Splitter, index) => connectTo(index, step)
  }
  def -->(product: Product) = to(product)

  def connectTo(index: Int, step: Transformer) = { step.connectFrom(index, this, 0); step }
  def connectTo(index: Int, step: Splitter) = { step.connectFrom(index, this, 0); step }

  protected def compute(args: Array[DataFrame], index: Int)(implicit ctx: SQLContext): DataFrame = compute(args(0), index)(ctx)

  protected def compute(arg: DataFrame, index: Int)(implicit ctx: SQLContext): DataFrame
}

/**
 * A step that has many inputs and one output.
 * The following members need to be implemented by subclasses:
 * +outputSchema: Option[StructType]
 * +compute(args: Array[DataFrame])(implicit ctx: SQLContext): DataFrame
 */
abstract class Merger(override val inputCount: Int) extends AbstractStep(inputCount, 1) with SingleOutput {

  protected def compute(args: Array[DataFrame], index: Int)(implicit ctx: SQLContext): DataFrame = compute(args)(ctx)

  protected def compute(args: Array[DataFrame])(implicit ctx: SQLContext): DataFrame
}