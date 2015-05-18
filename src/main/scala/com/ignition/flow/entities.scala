package com.ignition.flow

import scala.util.control.NonFatal
import scala.xml.Elem

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import com.ignition.SparkRuntime

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
   * Computes a step output value at the specified index. The count parameter, if set,
   * limits the output to the specified number of rows.
   * @throws FlowExecutionException in case of an error, or if the step is not connected.
   */
  @throws(classOf[FlowExecutionException])
  def output(index: Int, limit: Option[Int] = None)(implicit runtime: SparkRuntime): DataFrame

  /**
   * Returns the output schema of the step.
   */
  def outSchema(index: Int)(implicit runtime: SparkRuntime): StructType
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
 * +computeSchema(inSchemas: Array[Option[StructType]], index: Int)(implicit runtime: SparkRuntime): Option[StructType]
 * +compute(args: Array[DataFrame], index: Int)(implicit runtime: SparkRuntime): DataFrame
 */
abstract class AbstractStep(val inputCount: Int, val outputCount: Int) extends Step {
  protected[flow] val ins = Array.ofDim[(Step, Int)](inputCount)

  val allInputsRequired: Boolean = true

  /**
   * Connects an input port to an output port of another step.
   */
  private[flow] def connectFrom(inIndex: Int, step: Step, outIndex: Int): this.type = {
    assert(0 until step.outputCount contains outIndex, s"Output index out of range: $outIndex")
    ins(inIndex) = (step, outIndex)
    this
  }

  /**
   * Returns the output value at a given index by retrieving inputs and calling compute().
   */
  def output(index: Int, limit: Option[Int] = None)(implicit runtime: SparkRuntime): DataFrame = wrap {
    assert(0 until outputCount contains index, s"Output index out of range: $index")
    compute(inputs(limit), index, limit)
  }

  /**
   * Returns the output schema. It is a wrapper around computeSchema() to check the
   * index and provide error handling.
   */
  def outSchema(index: Int)(implicit runtime: SparkRuntime): StructType = wrap {
    assert(0 until outputCount contains index, s"Output index out of range: $index")
    computeSchema(index)
  }

  /**
   * Scans the input ports and retrieves the output values of the connectes steps.
   * The parameter 'limit', if set, specifies how many rows to fetch from each input.
   */
  protected def inputs(limit: Option[Int])(implicit runtime: SparkRuntime): Array[DataFrame] = (ins zipWithIndex) map {
    case ((step, index), _) => step.output(index, limit)(runtime)
    case (_, i) if allInputsRequired => throw FlowExecutionException(s"Input$i is not connected")
    case (_, _) => null
  }

  /**
   * Retrieves the input schemas
   */
  protected def inputSchemas(implicit runtime: SparkRuntime): Array[StructType] = (ins zipWithIndex) map {
    case ((step, index), _) => step.outSchema(index)(runtime)
    case (_, i) if allInputsRequired => throw FlowExecutionException(s"Input$i is not connected")
    case (_, _) => null
  }

  /**
   * Computes the data for output port with the specified index.
   */
  protected def compute(args: Array[DataFrame], index: Int, limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame

  /**
   * Computes the schema of the specified output.
   */
  protected def computeSchema(index: Int)(implicit runtime: SparkRuntime): StructType

  /**
   * Wraps exceptions into FlowExecutionException instances.
   */
  protected def wrap[T](body: => T): T = try { body } catch {
    case e: FlowExecutionException => throw e
    case NonFatal(e) => throw FlowExecutionException("Step computation failed", e)
  }

  /**
   * Returns the implicit SQLContext.
   */
  protected def ctx(implicit runtime: SparkRuntime) = runtime.ctx
  
  /**
   * Returns the implicit SparkContext.
   */
  protected def sparkContext(implicit runtime: SparkRuntime) = runtime.ctx.sparkContext

  /**
   * Optionally limits the data frame.
   */
  protected def optLimit(df: DataFrame, limit: Option[Int]) = limit map df.limit getOrElse df

  /**
   * Serialization helper. Used by subclasses in writeObject() method to explicitly
   * prohibit serialization.
   */
  protected def unserializable = throw new java.io.IOException("Object should not be serialized")
}

/**
 * A step with multiple output ports.
 */
trait MultiOutput { self: AbstractStep =>

  /**
   * Connects the output ports to multiple single input port nodes:
   * s to (a, b, c)
   */
  def to(tgtSteps: SingleInput*): Unit = tgtSteps.zipWithIndex foreach {
    case (step: SingleInput, index) => step.from(this, index)
  }

  /**
   * Connects the output ports to multiple single input port nodes:
   * s --> (a, b, c)
   */
  def -->(tgtSteps: SingleInput*): Unit = to(tgtSteps: _*)

  /**
   * Exposes the specified output port.
   */
  def out(outIndex: Int): this.OutPort = OutPort(outIndex)

  /**
   * The output port under the specified index.
   */
  protected[flow] case class OutPort(outIndex: Int) {
    val outer: self.type = self

    def to(step: SingleInput): step.type = step.from(outer, outIndex)
    def -->(step: SingleInput): step.type = to(step)

    def to(step: MultiInput): step.type = step.from(0, outer, outIndex)
    def -->(step: MultiInput): step.type = to(step)

    def to(in: MultiInput#InPort): Unit = in.outer.from(in.inIndex, outer, outIndex)
    def -->(in: MultiInput#InPort): Unit = to(in)
  }
}

/**
 * A step with a single output port.
 */
trait SingleOutput { self: AbstractStep =>
  def to(step: SingleInput): step.type = step.from(this)
  def -->(step: SingleInput): step.type = to(step)

  def to(in: MultiInput#InPort): Unit = in.outer.from(in.inIndex, this)
  def -->(in: MultiInput#InPort): Unit = to(in)

  def to(step: MultiInput): step.type = step.from(0, this)
  def -->(step: MultiInput): step.type = to(step)

  def -->(tgtIndex: Int) = SOutStepInIndex(this, tgtIndex)

  def output(implicit runtime: SparkRuntime): DataFrame = output(None)(runtime)
  def output(limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = output(0, limit)(runtime)

  def outSchema(implicit runtime: SparkRuntime): StructType = outSchema(0)(runtime)

  protected def input(limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = inputs(limit)(runtime)(0)
}

/**
 * A step with multiple input ports.
 */
trait MultiInput { self: AbstractStep =>
  private[flow] def from(inIndex: Int, step: Step with MultiOutput, outIndex: Int): this.type = connectFrom(inIndex, step, outIndex)

  private[flow] def from(inIndex: Int, step: Step with SingleOutput): this.type = connectFrom(inIndex, step, 0)

  /**
   * Exposes the input port under the specified index.
   */
  def in(inIndex: Int): this.InPort = InPort(inIndex)

  /**
   * The input port under the specified index.
   */
  protected[flow] case class InPort(inIndex: Int) { val outer: self.type = self }
}

/**
 * A step with a single input port.
 */
trait SingleInput { self: AbstractStep =>

  private[flow] def from(step: Step with MultiOutput, outIndex: Int): this.type = connectFrom(0, step, outIndex)

  private[flow] def from(step: Step with SingleOutput): this.type = connectFrom(0, step, 0)
}

/* connection classes */

private[flow] case class SOutStepInIndex(srcStep: Step with SingleOutput, inIndex: Int) {
  def :|(tgtStep: Step with MultiInput): tgtStep.type = tgtStep.from(inIndex, srcStep)
}

private[flow] case class OutInIndices(outIndex: Int, inIndex: Int) {
  def :|(tgtStep: Step with MultiInput) = MInStepOutInIndices(outIndex, inIndex, tgtStep)
}

private[flow] case class MInStepOutInIndices(outIndex: Int, inIndex: Int, tgtStep: Step with MultiInput) {
  def |:(srcStep: Step with MultiOutput) = tgtStep.from(inIndex, srcStep, outIndex)
}

private[flow] case class SInStepOutIndex(outIndex: Int, tgtStep: Step with SingleInput) {
  def |:(srcStep: Step with MultiOutput): tgtStep.type = tgtStep.from(srcStep, outIndex)
}

/**
 * A step that has one output and no inputs.
 * The following members need to be implemented by subclasses:
 * +computeSchema(implicit runtime: SparkRuntime): Option[StructType]
 * +compute(implicit runtime: SparkRuntime): DataFrame
 */
abstract class Producer extends AbstractStep(0, 1) with SingleOutput {

  protected def compute(args: Array[DataFrame], index: Int, limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame =
    compute(limit)(runtime)

  protected def compute(limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame

  protected def computeSchema(index: Int)(implicit runtime: SparkRuntime): StructType =
    computeSchema(runtime)

  protected def computeSchema(implicit runtime: SparkRuntime): StructType
}

/**
 * A step that has one input and one output.
 * The following members need to be implemented by subclasses:
 * +computeSchema(inSchema: Option[StructType])(implicit runtime: SparkRuntime): Option[StructType]
 * +compute(arg: DataFrame)(implicit runtime: SparkRuntime): DataFrame
 */
abstract class Transformer extends AbstractStep(1, 1) with SingleInput with SingleOutput {

  protected def compute(args: Array[DataFrame], index: Int, limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame =
    compute(args(0), limit)(runtime)

  protected def compute(arg: DataFrame, limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame

  protected def computeSchema(index: Int)(implicit runtime: SparkRuntime): StructType =
    computeSchema(inputSchemas(runtime)(0))(runtime)

  protected def computeSchema(inSchema: StructType)(implicit runtime: SparkRuntime): StructType
}

/**
 * A step that has many outputs and one input.
 * The following members need to be implemented by subclasses:
 * +computeSchema(inSchema: Option[StructType], index: Int)(implicit runtime: SparkRuntime): Option[StructType]
 * +compute(arg: DataFrame, index: Int)(implicit runtime: SparkRuntime): DataFrame
 */
abstract class Splitter(override val outputCount: Int)
  extends AbstractStep(1, outputCount) with SingleInput with MultiOutput {

  protected def compute(args: Array[DataFrame], index: Int, limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame =
    compute(args(0), index, limit)(runtime)

  protected def compute(arg: DataFrame, index: Int, limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame

  protected def computeSchema(index: Int)(implicit runtime: SparkRuntime): StructType =
    computeSchema(inputSchemas(runtime)(0), index)(runtime)

  protected def computeSchema(inSchema: StructType, index: Int)(implicit runtime: SparkRuntime): StructType
}

/**
 * A step that has many inputs and one output.
 * The following members need to be implemented by subclasses:
 * +computeSchema(inSchemas: Array[Option[StructType]])(implicit runtime: SparkRuntime): Option[StructType]
 * +compute(args: Array[DataFrame])(implicit runtime: SparkRuntime): DataFrame
 */
abstract class Merger(override val inputCount: Int)
  extends AbstractStep(inputCount, 1) with MultiInput with SingleOutput {

  protected def compute(args: Array[DataFrame], index: Int, limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame =
    compute(args, limit)(runtime)

  protected def compute(args: Array[DataFrame], limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame

  protected def computeSchema(index: Int)(implicit runtime: SparkRuntime): StructType =
    computeSchema(inputSchemas(runtime))(runtime)

  protected def computeSchema(inSchemas: Array[StructType])(implicit runtime: SparkRuntime): StructType
}

/**
 * A step with multiple input and output ports.
 * The following members need to be implemented by subclasses:
 * +computeSchema(inSchemas: Array[Option[StructType]], index: Int)(implicit runtime: SparkRuntime): Option[StructType]
 * +compute(args: Array[DataFrame], index: Int)(implicit runtime: SparkRuntime): DataFrame
 */
abstract class Module(override val inputCount: Int, override val outputCount: Int)
  extends AbstractStep(inputCount, outputCount) with MultiInput with MultiOutput {

  protected def computeSchema(index: Int)(implicit runtime: SparkRuntime): StructType =
    computeSchema(inputSchemas(runtime), index)(runtime)

  protected def computeSchema(inSchemas: Array[StructType], index: Int)(implicit runtime: SparkRuntime): StructType
}