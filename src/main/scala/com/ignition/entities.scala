package com.ignition

import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.xml.{ Elem, Node }

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
    case NonFatal(e)           => throw ExecutionException("Step computation failed", e)
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
 * Flow execution runtime.
 */
trait FlowRuntime

/**
 * A workflow step. It can have an arbitrary number of inputs and outputs, each of which
 * could be connected to inputs and outputs of other steps.
 * @param T the type encapsulating the data that is passed between steps.
 * @param R the type of the runtime context passed to the node for evaluation.
 */
trait Step[T, R <: FlowRuntime] extends AbstractStep with XmlExport with JsonExport {

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
   * Caches computed output values.
   */
  @transient private var cache = Map.empty[Int, T]

  /**
   * Clears the cache of this step and optionally that of its predecessors and descendants.
   */
  private[ignition] def resetCache(predecessors: Boolean, descendants: Boolean): Unit = synchronized {
    this.cache = Map.empty[Int, T]
    if (predecessors) for {
      tgt <- ins(this)
      src = tgt.inbound if src != null
      step = src.step if step != null
    } yield step.resetCache(true, false)
    if (descendants) for {
      src <- outs(this)
      tgt = src.outbound if tgt != null
      step = tgt.step if step != null
    } yield step.resetCache(false, true)
  }

  /**
   * Computes a step output value at the specified index. This method is invoked from output()
   * and can safely throw any exception, which will be wrapped into ExecutionException.
   * @param index the output value index.
   * @param preview if true, it indicates the preview mode. The implementation should make use of
   * this parameter to return a limited or simplified version of the output.
   */
  protected def compute(index: Int, preview: Boolean)(implicit runtime: R): T

  /**
   * Computes a step output value at the specified index.
   * @param index the output value index.
   * @param preview if true, it indicates the preview mode. The implementation should make use of
   * this parameter to return a limited or simplified version of the output.
   * @throws ExecutionException in case of an error, or if the step is not connected.
   */
  @throws(classOf[ExecutionException])
  final def output(index: Int, preview: Boolean)(implicit runtime: R): T = synchronized {
    cache.get(index) getOrElse wrap {
      assert(0 until outputCount contains index, s"Output index out of range: $index of $outputCount")
      notifyListeners(new BeforeStepComputed(this, index, preview))
      val result = compute(index, preview)
      notifyListeners(new AfterStepComputed(this, index, preview, result))
      if (!preview) cache += index -> result
      result
    }
  }

  /**
   * Shortcut for `output(index, false)`. Computes a step output at specified index.
   * @throws ExecutionException in case of an error, or if the step is not connected.
   */
  @throws(classOf[ExecutionException])
  final def output(index: Int)(implicit runtime: R): T = output(index, false)

  /**
   * Shortcut for `output(0, preview)`. Computes a step output at index 0.
   * @throws ExecutionException in case of an error, or if the step is not connected.
   */
  @throws(classOf[ExecutionException])
  final def output(preview: Boolean)(implicit runtime: R): T = output(0, preview)

  /**
   * Shortcut for `output(0, false)`. Computes a step output at index 0 with preview mode OFF.
   * @throws ExecutionException in case of an error, or if the step is not connected.
   */
  @throws(classOf[ExecutionException])
  final def output(implicit runtime: R): T = output(false)

  /**
   * Evaluates all step's outputs and returns a list of results.
   */
  @throws(classOf[ExecutionException])
  final def evaluate(implicit runtime: R): IndexedSeq[T] = 0 until outputCount map output

  /**
   * Listeners which will be notified on step events.
   */
  @transient private var listeners = Set.empty[StepListener[T, R]]

  /**
   * Registers a new listener.
   */
  def addStepListener(listener: StepListener[T, R]) = listeners += listener

  /**
   * Unregisters a listener.
   */
  def removeStepListener(listener: StepListener[T, R]) = listeners -= listener

  /**
   * Notifies all listeners.
   */
  private[ignition] def notifyListeners(event: StepEvent[T, R]) = event match {
    case e: StepConnectedFrom[T, R]  => listeners foreach (_.onStepConnectedFrom(e))
    case e: StepConnectedTo[T, R]    => listeners foreach (_.onStepConnectedTo(e))
    case e: BeforeStepComputed[T, R] => listeners foreach (_.onBeforeStepComputed(e))
    case e: AfterStepComputed[T, R]  => listeners foreach (_.onAfterStepComputed(e))
  }
}

/**
 * Something TO which a connection can be made, a connection target.
 */
trait ConnectionTarget[T, R <: FlowRuntime] {
  def step: Step[T, R]
  def index: Int

  private[ignition] var inbound: ConnectionSource[T, R] = null

  def from(src: ConnectionSource[T, R]): this.type = {
    val oldInbound = this.inbound

    val tgtStep = src.step
    val oldTgtOutbound = src.outbound

    this.inbound = src
    src.outbound = this

    if (this.step != null)
      this.step.notifyListeners(StepConnectedFrom(step, this, Option(oldInbound), this.inbound))

    if (tgtStep != null)
      tgtStep.notifyListeners(StepConnectedTo(tgtStep, src, Option(oldTgtOutbound), this))

    this
  }
}

/**
 * Something FROM which a connection can be made
 */
trait ConnectionSource[T, R <: FlowRuntime] {
  def step: Step[T, R]
  def index: Int

  private[ignition] var outbound: ConnectionTarget[T, R] = null

  def to(tgt: ConnectionTarget[T, R]): tgt.type = tgt.from(this)
  def -->(tgt: ConnectionTarget[T, R]): tgt.type = to(tgt)

  def to(tgt: MultiInputStep[T, R]): tgt.type = { to(tgt.in(0)); tgt }
  def -->(tgt: MultiInputStep[T, R]): tgt.type = to(tgt)

  def to(targets: ConnectionTarget[T, R]*): Unit = targets foreach (to(_))
  def -->(targets: ConnectionTarget[T, R]*): Unit = to(targets: _*)

  def value(preview: Boolean)(implicit runtime: R): T
}

/* inputs */
trait NoInputStep[T, R <: FlowRuntime] extends Step[T, R] {
  val inputCount = 0
}

trait SingleInputStep[T, R <: FlowRuntime] extends Step[T, R] with ConnectionTarget[T, R] {
  val step = this
  val index = 0
  val inputCount = 1
  def input(preview: Boolean)(implicit runtime: R) = {
    if (inbound == null && allInputsRequired) throw ExecutionException("Input is not connected")
    Option(inbound) map (_.value(preview)) getOrElse null.asInstanceOf[T]
  }
}

trait MultiInputStep[T, R <: FlowRuntime] extends Step[T, R] { self =>
  val in = new LazyArray[InPort](idx => InPort(idx))(inputCount)

  def inputs(preview: Boolean)(implicit runtime: R) = in.map { p =>
    for {
      port <- Option(p)
      ib <- Option(port.inbound)
    } yield ib.value(preview)
  }.zipWithIndex map {
    case (None, idx) if allInputsRequired => throw ExecutionException(s"Input$idx is not connected")
    case (x, _)                           => x getOrElse null.asInstanceOf[T]
  }

  case class InPort(index: Int) extends ConnectionTarget[T, R] {
    val step = self
    override def toString = s"$step.in($index)"
  }
}

/* outputs */
trait SingleOutputStep[T, R <: FlowRuntime] extends Step[T, R] with ConnectionSource[T, R] {
  val step = this
  val index = 0
  val outputCount = 1
  def outbounds(index: Int) = { assert(index == 0); this }
  def value(preview: Boolean)(implicit runtime: R): T = output(0, preview)
}

trait MultiOutputStep[T, R <: FlowRuntime] extends Step[T, R] { self =>
  val out = new LazyArray[OutPort](idx => OutPort(idx))(outputCount)

  def to(tgt: ConnectionTarget[T, R]): tgt.type = out(0).to(tgt)
  def -->(tgt: ConnectionTarget[T, R]): tgt.type = to(tgt)

  def to(tgt: MultiInputStep[T, R]): tgt.type = { to(tgt.in(0)); tgt }
  def -->(tgt: MultiInputStep[T, R]): tgt.type = to(tgt)

  def to(targets: ConnectionTarget[T, R]*): Unit = targets.zipWithIndex foreach {
    case (tgt, outIndex) => out(outIndex) to tgt
  }
  def -->(targets: ConnectionTarget[T, R]*): Unit = to(targets: _*)

  def outbounds(index: Int) = out(index)

  case class OutPort(index: Int) extends ConnectionSource[T, R] {
    val step = self
    def value(preview: Boolean)(implicit runtime: R) = self.output(index, preview)
    override def toString = s"$step.out($index)"
  }
}

/* templates */
abstract class Producer[T, R <: FlowRuntime] extends SingleOutputStep[T, R] with NoInputStep[T, R] {
  protected def compute(index: Int, preview: Boolean)(implicit runtime: R): T =
    compute(preview)
  protected def compute(preview: Boolean)(implicit runtime: R): T
}

abstract class Transformer[T, R <: FlowRuntime] extends SingleOutputStep[T, R] with SingleInputStep[T, R] {
  override val step = this
  override val index = 0
  protected def compute(index: Int, preview: Boolean)(implicit runtime: R): T =
    compute(input(preview), preview)
  protected def compute(arg: T, preview: Boolean)(implicit runtime: R): T
}

abstract class Splitter[T, R <: FlowRuntime] extends SingleInputStep[T, R] with MultiOutputStep[T, R] {
  protected def compute(index: Int, preview: Boolean)(implicit runtime: R): T =
    compute(input(preview), index, preview)
  protected def compute(arg: T, index: Int, preview: Boolean)(implicit runtime: R): T
}

abstract class Merger[T, R <: FlowRuntime] extends MultiInputStep[T, R] with SingleOutputStep[T, R] {
  protected def compute(index: Int, preview: Boolean)(implicit runtime: R): T =
    compute(inputs(preview), preview)
  protected def compute(args: IndexedSeq[T], preview: Boolean)(implicit runtime: R): T
}

abstract class Module[T, R <: FlowRuntime] extends MultiInputStep[T, R] with MultiOutputStep[T, R] {
  protected def compute(index: Int, preview: Boolean)(implicit runtime: R): T =
    compute(inputs(preview), index, preview)
  protected def compute(args: IndexedSeq[T], index: Int, preview: Boolean)(implicit runtime: R): T
}

/* listeners */

/**
 * Base trait for all step events.
 */
sealed trait StepEvent[T, R <: FlowRuntime] {
  def step: Step[T, R]
}

case class StepConnectedFrom[T, R <: FlowRuntime](
  step: Step[T, R], inPort: ConnectionTarget[T, R],
  oldInbound: Option[ConnectionSource[T, R]],
  newInbound: ConnectionSource[T, R]) extends StepEvent[T, R]

case class StepConnectedTo[T, R <: FlowRuntime](
  step: Step[T, R], outPort: ConnectionSource[T, R],
  oldOutbound: Option[ConnectionTarget[T, R]],
  newOutbound: ConnectionTarget[T, R]) extends StepEvent[T, R]

case class BeforeStepComputed[T, R <: FlowRuntime](
  step: Step[T, R], index: Int, preview: Boolean) extends StepEvent[T, R]

case class AfterStepComputed[T, R <: FlowRuntime](
  step: Step[T, R], index: Int, preview: Boolean, value: T) extends StepEvent[T, R]

/**
 * Listener which will be notified on step events.
 */
trait StepListener[T, R <: FlowRuntime] {

  /**
   * Called when the step's input port is connected to a new inbound.
   */
  def onStepConnectedFrom(event: StepConnectedFrom[T, R]) = {}

  /**
   * Called when the step's output port is connected to a new outbound.
   */
  def onStepConnectedTo(event: StepConnectedTo[T, R]) = {}

  /**
   * Called before the step value is computed.
   */
  def onBeforeStepComputed(event: BeforeStepComputed[T, R]) = {}

  /**
   * Called after the step value has been computed.
   */
  def onAfterStepComputed(event: AfterStepComputed[T, R]) = {}
}

/* XML serialization */

/**
 * Converts an entity into XML.
 */
trait XmlExport {
  def toXml: Elem
}

/**
 * Restores a flow step from XML.
 * @param S step class.
 * @param T type encapsulating the data that is passed between steps.
 * @param R type of the runtime context passed to the node for evaluation.
 */
trait XmlStepFactory[S <: Step[T, R], T, R <: FlowRuntime] {
  def fromXml(xml: Node): S
}

/* JSON serialization */

/**
 * Converts an entity into JSON.
 */
trait JsonExport {
  def toJson: JValue
}

/**
 * Restores a flow step from JSON.
 * @param S step class.
 * @param T type encapsulating the data that is passed between steps.
 * @param R type of the runtime context passed to the node for evaluation.
 */
trait JsonStepFactory[S <: Step[T, R], T, R <: FlowRuntime] {
  def fromJson(json: JValue): S
}