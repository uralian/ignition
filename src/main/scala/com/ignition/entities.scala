package com.ignition

import scala.util.control.NonFatal
import scala.xml.Elem

import org.apache.spark.sql.types.StructType

/**
 * A workflow step. It can have an arbitrary number of inputs and outputs.
 * @param T the type parameter encapsulating the data that is passed between steps.
 */
trait Step[T] {

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
   * @throws ExecutionException in case of an error, or if the step is not connected.
   */
  @throws(classOf[ExecutionException])
  def output(index: Int, limit: Option[Int] = None)(implicit runtime: SparkRuntime): T

  /**
   * Returns the output schema of the step.
   */
  def outSchema(index: Int)(implicit runtime: SparkRuntime): StructType
}

/**
 * An abstract implementation base class for Step trait.
 * The following members need to be implemented by subclasses:
 * +computeSchema(inSchemas: Seq[Option[StructType]], index: Int)(implicit runtime: SparkRuntime): Option[StructType]
 * +compute(args: Seq[DataFrame], index: Int)(implicit runtime: SparkRuntime): T
 */
abstract class AbstractStep[T](val inputCount: Int, val outputCount: Int) extends Step[T] {
  protected[ignition] val ins = Array.ofDim[(Step[T], Int)](inputCount)

  val allInputsRequired: Boolean = true

  /**
   * Connects an input port to an output port of another step.
   */
  def connectFrom(inIndex: Int, step: Step[T], outIndex: Int): AbstractStep.this.type = {
    assert(0 until step.outputCount contains outIndex, s"Output index out of range: $outIndex")
    ins(inIndex) = (step, outIndex)
    AbstractStep.this
  }

  /**
   * Returns the output value at a given index by retrieving inputs and calling compute().
   */
  def output(index: Int, limit: Option[Int] = None)(implicit runtime: SparkRuntime): T = wrap {
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
  protected def inputs(limit: Option[Int])(implicit runtime: SparkRuntime) = (ins zipWithIndex) map {
    case ((step, index), _) => step.output(index, limit)(runtime)
    case (_, i) if allInputsRequired => throw ExecutionException(s"Input$i is not connected")
    case (_, _) => null.asInstanceOf[T]
  } toSeq

  /**
   * Retrieves the input schemas
   */
  protected def inputSchemas(implicit runtime: SparkRuntime) = (ins zipWithIndex) map {
    case ((step, index), _) => step.outSchema(index)(runtime)
    case (_, i) if allInputsRequired => throw ExecutionException(s"Input$i is not connected")
    case (_, _) => null
  }

  /**
   * Computes the data for output port with the specified index.
   */
  protected def compute(args: Seq[T], index: Int, limit: Option[Int])(implicit runtime: SparkRuntime): T

  /**
   * Computes the schema of the specified output.
   */
  protected def computeSchema(index: Int)(implicit runtime: SparkRuntime): StructType

  /**
   * Wraps exceptions into ExecutionException instances.
   */
  protected def wrap[U](body: => U): U = try { body } catch {
    case e: ExecutionException => throw e
    case NonFatal(e) => throw ExecutionException("Step computation failed", e)
  }

  /**
   * Serialization helper. Used by subclasses in writeObject() method to explicitly
   * prohibit serialization.
   */
  protected def unserializable = throw new java.io.IOException("Object should not be serialized")
}

/**
 * A step with multiple output ports.
 */
trait MultiOutput[T] { self: AbstractStep[T] =>

  /**
   * Connects the output ports to multiple single input port nodes:
   * s to (a, b, c)
   */
  def to(tgtSteps: SingleInput[T]*): Unit = tgtSteps.zipWithIndex foreach {
    case (step: SingleInput[T], index) => step.from(this, index)
  }

  /**
   * Connects the output ports to multiple single input port nodes:
   * s --> (a, b, c)
   */
  def -->(tgtSteps: SingleInput[T]*): Unit = to(tgtSteps: _*)

  /**
   * Exposes the specified output port.
   */
  def out(outIndex: Int): this.OutPort = OutPort(outIndex)

  /**
   * The output port under the specified index.
   */
  protected[ignition] case class OutPort(outIndex: Int) {
    val outer: self.type = self

    def to(step: SingleInput[T]): step.type = step.from(outer, outIndex)
    def -->(step: SingleInput[T]): step.type = to(step)

    def to(step: MultiInput[T]): step.type = step.from(0, outer, outIndex)
    def -->(step: MultiInput[T]): step.type = to(step)

    def to(in: MultiInput[T]#InPort): Unit = in.outer.from(in.inIndex, outer, outIndex)
    def -->(in: MultiInput[T]#InPort): Unit = to(in)
  }
}

/**
 * A step with a single output port.
 */
trait SingleOutput[T] { self: AbstractStep[T] =>
  def to(step: SingleInput[T]): step.type = step.from(this)
  def -->(step: SingleInput[T]): step.type = to(step)

  def to(in: MultiInput[T]#InPort): Unit = in.outer.from(in.inIndex, this)
  def -->(in: MultiInput[T]#InPort): Unit = to(in)

  def to(step: MultiInput[T]): step.type = step.from(0, this)
  def -->(step: MultiInput[T]): step.type = to(step)

  def -->(tgtIndex: Int) = SOutStepInIndex(this, tgtIndex)

  def output(implicit runtime: SparkRuntime): T = output(None)(runtime)
  def output(limit: Option[Int])(implicit runtime: SparkRuntime): T = output(0, limit)(runtime)

  def outSchema(implicit runtime: SparkRuntime): StructType = outSchema(0)(runtime)

  protected def input(limit: Option[Int])(implicit runtime: SparkRuntime): T = inputs(limit)(runtime)(0)
}

/**
 * A step with multiple input ports.
 */
trait MultiInput[T] { self: AbstractStep[T] =>
  private[ignition] def from(inIndex: Int, step: Step[T] with MultiOutput[T], outIndex: Int): this.type = connectFrom(inIndex, step, outIndex)

  private[ignition] def from(inIndex: Int, step: Step[T] with SingleOutput[T]): this.type = connectFrom(inIndex, step, 0)

  /**
   * Exposes the input port under the specified index.
   */
  def in(inIndex: Int): this.InPort = InPort(inIndex)

  /**
   * The input port under the specified index.
   */
  protected[ignition] case class InPort(inIndex: Int) { val outer: self.type = self }
}

/**
 * A step with a single input port.
 */
trait SingleInput[T] { self: AbstractStep[T] =>

  private[ignition] def from(step: Step[T] with MultiOutput[T], outIndex: Int): this.type = connectFrom(0, step, outIndex)

  private[ignition] def from(step: Step[T] with SingleOutput[T]): this.type = connectFrom(0, step, 0)
}

/* connection classes */

private[ignition] case class SOutStepInIndex[T](srcStep: Step[T] with SingleOutput[T], inIndex: Int) {
  def :|(tgtStep: Step[T] with MultiInput[T]): tgtStep.type = tgtStep.from(inIndex, srcStep)
}

private[ignition] case class OutInIndices(outIndex: Int, inIndex: Int) {
  def :|[T](tgtStep: Step[T] with MultiInput[T]) = MInStepOutInIndices[T](outIndex, inIndex, tgtStep)
}

private[ignition] case class MInStepOutInIndices[T](outIndex: Int, inIndex: Int, tgtStep: Step[T] with MultiInput[T]) {
  def |:(srcStep: Step[T] with MultiOutput[T]) = tgtStep.from(inIndex, srcStep, outIndex)
}

private[ignition] case class SInStepOutIndex[T](outIndex: Int, tgtStep: Step[T] with SingleInput[T]) {
  def |:(srcStep: Step[T] with MultiOutput[T]): tgtStep.type = tgtStep.from(srcStep, outIndex)
}

/* step templates */

/**
 * A step that has one output and no inputs.
 * The following members need to be implemented by subclasses:
 * +computeSchema(implicit runtime: SparkRuntime): Option[StructType]
 * +compute(implicit runtime: SparkRuntime): T
 */
abstract class Producer[T] extends AbstractStep[T](0, 1) with SingleOutput[T] {

  protected def compute(args: Seq[T], index: Int, limit: Option[Int])(implicit runtime: SparkRuntime): T =
    compute(limit)(runtime)

  protected def compute(limit: Option[Int])(implicit runtime: SparkRuntime): T

  protected def computeSchema(index: Int)(implicit runtime: SparkRuntime): StructType =
    computeSchema(runtime)

  protected def computeSchema(implicit runtime: SparkRuntime): StructType
}

/**
 * A step that has one input and one output.
 * The following members need to be implemented by subclasses:
 * +computeSchema(inSchema: Option[StructType])(implicit runtime: SparkRuntime): Option[StructType]
 * +compute(arg: T)(implicit runtime: SparkRuntime): T
 */
abstract class Transformer[T] extends AbstractStep[T](1, 1) with SingleInput[T] with SingleOutput[T] {

  protected def compute(args: Seq[T], index: Int, limit: Option[Int])(implicit runtime: SparkRuntime): T =
    compute(args(0), limit)(runtime)

  protected def compute(arg: T, limit: Option[Int])(implicit runtime: SparkRuntime): T

  protected def computeSchema(index: Int)(implicit runtime: SparkRuntime): StructType =
    computeSchema(inputSchemas(runtime)(0))(runtime)

  protected def computeSchema(inSchema: StructType)(implicit runtime: SparkRuntime): StructType
}

/**
 * A step that has many outputs and one input.
 * The following members need to be implemented by subclasses:
 * +computeSchema(inSchema: Option[StructType], index: Int)(implicit runtime: SparkRuntime): Option[StructType]
 * +compute(arg: T, index: Int)(implicit runtime: SparkRuntime): T
 */
abstract class Splitter[T](override val outputCount: Int)
  extends AbstractStep[T](1, outputCount) with SingleInput[T] with MultiOutput[T] {

  protected def compute(args: Seq[T], index: Int, limit: Option[Int])(implicit runtime: SparkRuntime): T =
    compute(args(0), index, limit)(runtime)

  protected def compute(arg: T, index: Int, limit: Option[Int])(implicit runtime: SparkRuntime): T

  protected def computeSchema(index: Int)(implicit runtime: SparkRuntime): StructType =
    computeSchema(inputSchemas(runtime)(0), index)(runtime)

  protected def computeSchema(inSchema: StructType, index: Int)(implicit runtime: SparkRuntime): StructType
}

/**
 * A step that has many inputs and one output.
 * The following members need to be implemented by subclasses:
 * +computeSchema(inSchemas: Seq[StructType])(implicit runtime: SparkRuntime): Option[StructType]
 * +compute(args: Seq[T])(implicit runtime: SparkRuntime): T
 */
abstract class Merger[T](override val inputCount: Int)
  extends AbstractStep[T](inputCount, 1) with MultiInput[T] with SingleOutput[T] {

  protected def compute(args: Seq[T], index: Int, limit: Option[Int])(implicit runtime: SparkRuntime): T =
    compute(args, limit)(runtime)

  protected def compute(args: Seq[T], limit: Option[Int])(implicit runtime: SparkRuntime): T

  protected def computeSchema(index: Int)(implicit runtime: SparkRuntime): StructType =
    computeSchema(inputSchemas(runtime))(runtime)

  protected def computeSchema(inSchemas: Seq[StructType])(implicit runtime: SparkRuntime): StructType
}

/**
 * A step with multiple input and output ports.
 * The following members need to be implemented by subclasses:
 * +computeSchema(inSchemas: Seq[StructType], index: Int)(implicit runtime: SparkRuntime): Option[StructType]
 * +compute(args: Seq[T], index: Int)(implicit runtime: SparkRuntime): T
 */
abstract class Module[T](override val inputCount: Int, override val outputCount: Int)
  extends AbstractStep[T](inputCount, outputCount) with MultiInput[T] with MultiOutput[T] {

  protected def computeSchema(index: Int)(implicit runtime: SparkRuntime): StructType =
    computeSchema(inputSchemas(runtime), index)(runtime)

  protected def computeSchema(inSchemas: Seq[StructType], index: Int)(implicit runtime: SparkRuntime): StructType
}

/**
 * XML serialization.
 */
trait XmlExport {
  def toXml: Elem
}
