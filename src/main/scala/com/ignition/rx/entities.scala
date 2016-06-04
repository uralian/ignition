package com.ignition.rx

import scala.collection.mutable.ArrayBuffer

import com.ignition.util.Logging

import rx.lang.scala.{ Observable, Subscription }
import rx.lang.scala.subjects.BehaviorSubject

/**
 * A block that emits items of type Observable[R].
 */
trait RxBlock[+R] {
  /**
   * Returns the block's output as an Observable.
   */
  def output: Observable[R]

  /**
   * Restarts the block.
   */
  def reset(): Unit

  /**
   * Stops the data flow through the block.
   */
  def shutdown(): Unit
}

/**
 * Top of RxBlock hierarchy.
 *
 * @param A the type of the attribute set (a single class or a tuple).
 * @param T the type of the inputs: a single Observable or a collection of them.
 * @param R the type of the result.
 */
abstract class AbstractRxBlock[A, T, R](evaluator: A => T => Observable[R])
    extends RxBlock[R] with Logging { self =>

  /**
   * Combines the attribute streams into a single observable.
   */
  protected def combineAttributes: Observable[A]

  /**
   * Returns the input streams.
   */
  protected def inputs: T

  private val subj = BehaviorSubject[R]()
  private var outSubscription: Option[Subscription] = None
  private var attrSubscription: Option[Subscription] = None

  lazy val output = withEvents("output")(subj).share

  /**
   * Resets the block by renewing the subscriptions and re-initiating the sequence.
   */
  def reset() = {
    unsubsribeOutput
    unsubscribeAttributes

    attrSubscription = Some(combineAttributes subscribe { attrs =>
      unsubsribeOutput
      outSubscription = Some(evaluator(attrs)(inputs) subscribe (subj.onNext(_)))
    })
  }

  /**
   * Cancels all subscriptions and stops emitting items. 
   */
  def shutdown() = {
    unsubsribeOutput
    unsubscribeAttributes
    subj.onCompleted
  }

  /**
   * Connects the output of this block to an input port of another block.
   */
  def bind(port: AbstractRxBlock[_, _, _]#Port[R]) = port.bind(this)
  
  /**
   * Connects the output of this block to an input port of another block.
   * An alias for `bind(port)`.
   */
  def ~>(port: AbstractRxBlock[_, _, _]#Port[R]) = bind(port)

  protected def unsubscribeAttributes() = attrSubscription foreach (_.unsubscribe)
  protected def unsubsribeOutput() = outSubscription foreach (_.unsubscribe)

  protected def withEvents(name: String)(stream: Observable[R]): Observable[R] = {
    def render(state: String) = debug(s"[$id.$name] $state")

    stream
      .doOnCompleted(render("completed"))
      .doOnSubscribe(render("subscribed to"))
      .doOnTerminate(render("terminated"))
      .doOnUnsubscribe(render("unsubscribed from"))
  }

  protected val NO_ATTRIBUTES = Observable.just({})

  protected val NO_INPUTS = {}

  protected val id = getClass.getSimpleName + (math.abs(hashCode) % 1000)

  /**
   * Connector for attributes and inputs.
   */
  case class Port[X](name: String) {
    val owner = self

    var in: Observable[X] = Observable.never

    /**
     * Assign a specific value to this port.
     */
    def set(value: X) = synchronized {
      in = Observable.just(value)
      info(s"$id.$name set to $value")
      reset
    }

    /**
     * Assign a specific value to this port.
     * An alias for `set(value)`.
     */
    def <~(value: X) = set(value)

    /**
     * Connects this port to the output of another block.
     */
    def bind(block: AbstractRxBlock[_, _, X]) = synchronized {
      in = block.output
      info(s"$id.$name bound to ${block.id}.output")
      reset
    }

    /**
     * Connects this port to the output of another block.
     * An alias for `bind(block)`.
     */
    def <~(block: AbstractRxBlock[_, _, X]) = bind(block)

    /**
     * Unbinds this port so that it never emits any values.
     */
    def unbind() = synchronized {
      in = Observable.never
      info(s"$id.$name unbound")
      reset
    }
  }

  /**
   * Connector for a list of attributes or inputs.
   */
  case class PortList[X](name: String) extends IndexedSeq[Port[X]] {
    val ports = ArrayBuffer.empty[Port[X]]

    /**
     * Returns the specified port.
     */
    def apply(index: Int) = ports(index)

    /**
     * Returns the number of ports in the list.
     */
    def length = ports.length

    /**
     * Adds and returns a new port.
     */
    def add() = {
      val index = ports.length
      val port = Port[X](s"$name($index)")
      ports += port
      info(s"$id.$name added port $index")
      port
    }

    /**
     * Removes the port at the specified index.
     */
    def remove(index: Int) = {
      ports.remove(index)
      info(s"$id.$name($index) port removed")
      reset
    }
  }
}

/**
 * A block that does not have any inputs.
 */
abstract class RxProducer[A, R](evaluator: A => Observable[R]) extends AbstractRxBlock[A, Unit, R](
  (attrs: A) => (x: Unit) => evaluator(attrs))

/**
 * A block that has one input.
 */
abstract class RxTransformer[A, T, R](evaluator: A => Observable[T] => Observable[R])
  extends AbstractRxBlock[A, Observable[T], R](evaluator)

/**
 * A block that has two inputs.
 */
abstract class RxMerger[A, T1, T2, R](evaluator: A => (Observable[T1], Observable[T2]) => Observable[R])
  extends AbstractRxBlock[A, (Observable[T1], Observable[T2]), R](
    (attrs: A) => (tuple: (Observable[T1], Observable[T2])) => evaluator(attrs)(tuple._1, tuple._2))

/**
 * A block that has a list of inputs of the same type.
 */
abstract class RxMergerN[A, T, R](evaluator: A => Seq[Observable[T]] => Observable[R])
  extends AbstractRxBlock[A, Seq[Observable[T]], R](evaluator)
