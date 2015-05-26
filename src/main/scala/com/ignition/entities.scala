package com.ignition

import scala.util.control.NonFatal

import org.apache.spark.sql.types.StructType

/**
 * A workflow step. It can have an arbitrary number of inputs and outputs.
 * @param T the type parameter encapsulating the data that is passed between steps.
 */
trait XStep[T] {

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
 * +computeSchema(inSchemas: Array[Option[StructType]], index: Int)(implicit runtime: SparkRuntime): Option[StructType]
 * +compute(args: Array[DataFrame], index: Int)(implicit runtime: SparkRuntime): DataFrame
 */
abstract class AbstractXStep[T](val inputCount: Int, val outputCount: Int) extends XStep[T] {
  protected[ignition] val ins = Array.ofDim[(XStep[T], Int)](inputCount)

  val allInputsRequired: Boolean = true

  /**
   * Connects an input port to an output port of another step.
   */
  protected[ignition] def connectFrom(inIndex: Int, step: XStep[T], outIndex: Int): this.type = {
    assert(0 until step.outputCount contains outIndex, s"Output index out of range: $outIndex")
    ins(inIndex) = (step, outIndex)
    this
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
  protected def inputSchemas(implicit runtime: SparkRuntime): Array[StructType] = (ins zipWithIndex) map {
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