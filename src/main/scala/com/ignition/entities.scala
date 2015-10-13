package com.ignition

import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.xml.Elem

import org.json4s.JValue

/**
 * A wrapper around an array that lazily initialies an element of the array when it is accessed
 * the first time.
 * @param the type of array elements.
 * @param builder a function that is called when the element with a given index does not yet exist.
 * @param length the size of the array.
 */
private[ignition] class LazyArray[A: ClassTag](builder: Int => A)(val length: Int)
  extends IndexedSeq[A] with Serializable {

  private val buffer = Array.ofDim[A](length)

  def apply(idx: Int): A = Option(buffer(idx)) getOrElse {
    buffer(idx) = builder(idx)
    buffer(idx)
  }
}

/**
 * Provides base step functionality and controls serializability of the steps.
 */
abstract class AbstractStep extends Serializable {
  /**
   * Wraps exceptions into ExecutionException instances.
   */
  final protected def wrap[U](body: => U): U = try { body } catch {
    case e: ExecutionException => throw e
    case NonFatal(e) => throw ExecutionException("Step computation failed", e)
  }

  /**
   * Serialization helper. Enables or disables step serialization based on the environment property.
   */
  private def writeObject(out: java.io.ObjectOutputStream): Unit =
    if (System.getProperty(STEPS_SERIALIZABLE) == false.toString)
      throw new java.io.NotSerializableException("Steps should not be serialized")
    else
      out.defaultWriteObject
}

/**
 * A workflow step. It can have an arbitrary number of inputs and outputs, each of which
 * could be connected to inputs and outputs of other steps.
 * @param T the type parameter encapsulating the data that is passed between steps.
 */
trait Step[T] extends AbstractStep with XmlExport with JsonExport {

  /**
   * The maximum number of input ports.
   */
  def inputCount: Int

  /**
   * The number of output ports.
   */
  def outputCount: Int

  /**
   * Specifies if the step should throw an error if one of the inputs is not connected.
   */
  def allInputsRequired = true

  /**
   * Computes a step output value at the specified index. This method is invoked from output()
   * and can safely throw any exception, which will be wrapped into ExecutionException.
   * @param index the output value index.
   * @param preview if true, it indicates the preview mode. The implementation should make use of
   * this parameter to return a limited or simplified version of the output.
   */
  protected def compute(index: Int, preview: Boolean)(implicit runtime: SparkRuntime): T

  /**
   * Computes a step output value at the specified index.
   * @param index the output value index.
   * @param preview if true, it indicates the preview mode. The implementation should make use of
   * this parameter to return a limited or simplified version of the output.
   * @throws ExecutionException in case of an error, or if the step is not connected.
   */
  @throws(classOf[ExecutionException])
  final def output(index: Int, preview: Boolean)(implicit runtime: SparkRuntime): T = wrap {
    assert(0 until outputCount contains index, s"Output index out of range: $index of $outputCount")
    compute(index, preview)
  }

  /**
   * Shortcut for [[output(0, false)]]. Computes a step output at index 0.
   * @throws ExecutionException in case of an error, or if the step is not connected.
   */
  @throws(classOf[ExecutionException])
  final def output(preview: Boolean)(implicit runtime: SparkRuntime): T = output(0, preview)

  /**
   * Shortcut for [[output(false)]]. Computes a step output at index 0 with preview mode OFF.
   * @throws ExecutionException in case of an error, or if the step is not connected.
   */
  @throws(classOf[ExecutionException])
  final def output(implicit runtime: SparkRuntime): T = output(false)
}

/**
 * Something TO which a connection can be made, a connection target.
 */
trait ConnectionTarget[T] {
  def step: Step[T]
  def index: Int

  var inbound: ConnectionSource[T] = null
  def from(src: ConnectionSource[T]): this.type = { this.inbound = src; this }
}

/**
 * Something FROM which a connection can be made
 */
trait ConnectionSource[T] {
  def step: Step[T]
  def index: Int

  def to(tgt: ConnectionTarget[T]): tgt.type = tgt.from(this)
  def -->(tgt: ConnectionTarget[T]): tgt.type = to(tgt)

  def to(tgt: MultiInputStep[T]): tgt.type = { to(tgt.in(0)); tgt }
  def -->(tgt: MultiInputStep[T]): tgt.type = to(tgt)

  def to(targets: ConnectionTarget[T]*): Unit = targets foreach (to(_))
  def -->(targets: ConnectionTarget[T]*): Unit = to(targets: _*)

  def value(preview: Boolean)(implicit runtime: SparkRuntime): T
}

/* inputs */
trait NoInputStep[T] extends Step[T] {
  val inputCount = 0
}

trait SingleInputStep[T] extends Step[T] with ConnectionTarget[T] {
  val step = this
  val index = 0
  val inputCount = 1
  def input(preview: Boolean)(implicit runtime: SparkRuntime) = {
    if (inbound == null && allInputsRequired) throw ExecutionException("Input is not connected")
    Option(inbound) map (_.value(preview)) getOrElse null.asInstanceOf[T]
  }
}

trait MultiInputStep[T] extends Step[T] { self =>
  val in = new LazyArray[InPort](idx => InPort(idx))(inputCount)

  def inputs(preview: Boolean)(implicit runtime: SparkRuntime) = in.map { p =>
    for {
      port <- Option(p)
      ib <- Option(port.inbound)
    } yield ib.value(preview)
  }.zipWithIndex map {
    case (None, idx) if allInputsRequired => throw ExecutionException(s"Input$idx is not connected")
    case (x, _) => x getOrElse null.asInstanceOf[T]
  }

  case class InPort(index: Int) extends ConnectionTarget[T] {
    val step = self
    override def toString = s"$step.in($index)"
  }
}

/* outputs */
trait SingleOutputStep[T] extends Step[T] with ConnectionSource[T] {
  val step = this
  val index = 0
  val outputCount = 1
  def outbounds(index: Int) = { assert(index == 0); this }
  def value(preview: Boolean)(implicit runtime: SparkRuntime): T = output(0, preview)
}

trait MultiOutputStep[T] extends Step[T] { self =>
  val out = new LazyArray[OutPort](idx => OutPort(idx))(outputCount)

  def to(tgt: ConnectionTarget[T]): tgt.type = out(0).to(tgt)
  def -->(tgt: ConnectionTarget[T]): tgt.type = to(tgt)

  def to(tgt: MultiInputStep[T]): tgt.type = { to(tgt.in(0)); tgt }
  def -->(tgt: MultiInputStep[T]): tgt.type = to(tgt)

  def to(targets: ConnectionTarget[T]*): Unit = targets.zipWithIndex foreach {
    case (tgt, outIndex) => out(outIndex) to tgt
  }
  def -->(targets: ConnectionTarget[T]*): Unit = to(targets: _*)

  def outbounds(index: Int) = out(index)

  case class OutPort(index: Int) extends ConnectionSource[T] {
    val step = self
    def value(preview: Boolean)(implicit runtime: SparkRuntime) = self.output(index, preview)
    override def toString = s"$step.out($index)"
  }
}

/* templates */
abstract class Producer[T] extends SingleOutputStep[T] with NoInputStep[T] {
  protected def compute(index: Int, preview: Boolean)(implicit runtime: SparkRuntime): T =
    compute(preview)
  protected def compute(preview: Boolean)(implicit runtime: SparkRuntime): T
}

abstract class Transformer[T] extends SingleOutputStep[T] with SingleInputStep[T] {
  override val step = this
  override val index = 0
  protected def compute(index: Int, preview: Boolean)(implicit runtime: SparkRuntime): T =
    compute(input(preview), preview)
  protected def compute(arg: T, preview: Boolean)(implicit runtime: SparkRuntime): T
}

abstract class Splitter[T] extends SingleInputStep[T] with MultiOutputStep[T] {
  protected def compute(index: Int, preview: Boolean)(implicit runtime: SparkRuntime): T =
    compute(input(preview), index, preview)
  protected def compute(arg: T, index: Int, preview: Boolean)(implicit runtime: SparkRuntime): T
}

abstract class Merger[T] extends MultiInputStep[T] with SingleOutputStep[T] {
  protected def compute(index: Int, preview: Boolean)(implicit runtime: SparkRuntime): T =
    compute(inputs(preview), preview)
  protected def compute(args: IndexedSeq[T], preview: Boolean)(implicit runtime: SparkRuntime): T
}

abstract class Module[T] extends MultiInputStep[T] with MultiOutputStep[T] {
  protected def compute(index: Int, preview: Boolean)(implicit runtime: SparkRuntime): T =
    compute(inputs(preview), index, preview)
  protected def compute(args: IndexedSeq[T], index: Int, preview: Boolean)(implicit runtime: SparkRuntime): T
}

/**
 * XML serialization.
 */
trait XmlExport {
  def toXml: Elem
}

/**
 * JSON serialization.
 */
trait JsonExport {
  def toJson: JValue
}