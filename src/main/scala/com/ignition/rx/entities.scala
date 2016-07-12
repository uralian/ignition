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
   * Returns the block's output as an Observable. This observable is stable in the sense that
   * it keeps emitting items, whether the block has been reset, or its inputs changed etc., i.e.
   * it never calls `onError` or `onCompleted` methods on its subscribers until the block's
   * `shutdown()` method is called.
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
 */
abstract class AbstractRxBlock[R] extends RxBlock[R] with Logging { self =>

  /**
   * computes the block's output
   */
  protected def compute: Observable[R]

  /**
   * Ports connected to this block's output.
   */
  private val targets = collection.mutable.Set.empty[AbstractRxBlock[_]#Port[_ >: R]]

  /**
   * "stable" observable, will never get onError or onCompleted
   */
  private val subj = BehaviorSubject[R]()
  lazy val output = withEvents("output")(subj).share

  /**
   * Output subscription.
   */
  private var obs: Observable[R] = Observable.never
  private var subscription: Option[Subscription] = None

  /**
   * Resets the block by renewing the subscriptions and re-initiating the sequence.
   */
  def reset() = synchronized {
    unsubsribeOutput

    obs = withEvents("output")(compute).share
    subscription = Some(obs subscribe (subj.onNext(_)))

    targets foreach (_.bind(obs))
    targets map (_.owner) foreach (_.reset)
  }

  /**
   * Cancels all subscriptions and stops emitting items.
   */
  def shutdown() = synchronized {
    unsubsribeOutput
    subj.onCompleted
  }

  /**
   * Connects the output of this block to an input port of another block.
   */
  def to[T](port: AbstractRxBlock[T]#Port[_ >: R]): port.owner.type = synchronized {
    targets += port
    port.bind(obs)

    info(s"${port.owner.id}.${port.name} bound to $id.output")
    port.owner
  }

  /**
   * Connects the output of this block to an input port of another block.
   * An alias for `to(port)`.
   */
  def ~>[T](port: AbstractRxBlock[T]#Port[_ >: R]) = to(port)

  /**
   * Connects the output of this block to `source` input port of a transformer block.
   */
  def to[U](block: RxTransformer[_ >: R, U]): block.type = {
    to(block.source)
    block
  }

  /**
   * Connects the output of this block to `source` input port of a transformer block.
   * An alias for `to(block)`.
   */
  def ~>[U](block: RxTransformer[_ >: R, U]): block.type = to(block)

  /**
   * Cancels the output subscription.
   */
  protected def unsubsribeOutput() = subscription foreach (_.unsubscribe)

  /**
   * Decorates the observable by adding listeners for its lifecycle events.
   */
  protected def withEvents(name: String)(stream: Observable[R]): Observable[R] = {
    def render(state: String) = debug(s"[$id.$name] $state")

    stream
      .doOnCompleted(render("completed"))
      .doOnSubscribe(render("subscribed to"))
      .doOnTerminate(render("terminated"))
      .doOnUnsubscribe(render("unsubscribed from"))
  }

  /**
   * Generates block's id for logging.
   */
  protected val id = getClass.getSimpleName + (math.abs(hashCode) % 1000)

  /**
   * Connector for attributes and inputs. Provides the input as Observable[X].
   */
  case class Port[X](name: String) {
    val owner = self

    private var _in: Observable[X] = Observable.never

    /**
     * Block's input as Observable[X].
     */
    def in = _in

    /**
     * Binds this port to an observable.
     */
    private[rx] def bind(source: Observable[X]) = synchronized {
      _in = source
    }

    /**
     * Assign a specific value to this port.
     */
    def set(value: X) = synchronized {
      _in = Observable.just(value)
      info(s"$id.$name set to $value")
    }

    /**
     * Assign a specific value to this port.
     * An alias for `set(value)`.
     */
    def <~(value: X) = set(value)

    /**
     * Connects this port to the output of another block.
     */
    def from(block: AbstractRxBlock[_ <: X]): Unit = block to this

    /**
     * Connects this port to the output of another block.
     * An alias for `from(block)`.
     */
    def <~(block: AbstractRxBlock[_ <: X]) = from(block)

    /**
     * Disconnects the port from any source so that it never emits any values.
     */
    def unbind() = synchronized {
      _in = Observable.never
    }
  }

  /**
   * Connector for a list of attributes or inputs.
   */
  case class PortList[X](name: String) extends IndexedSeq[Port[X]] {
    val ports = ArrayBuffer.empty[Port[X]]

    /**
     * Returns a collection of input observables, one from each port.
     */
    def ins = ports map (_.in)

    /**
     * Returns the specified port.
     */
    def apply(index: Int) = ports(index)

    /**
     * Returns the number of ports in the list.
     */
    def length = ports.length

    /**
     * Adds a new port to the list.
     */
    def add(): Port[X] = {
      val index = ports.length
      val port = Port[X](s"$name($index)")
      ports += port
      info(s"$id.$name added port $index")
      port
    }

    /**
     * Adds a few new ports to the list.
     */
    def add(count: Int): Unit = (1 to count) foreach (_ => add)

    /**
     * Clears the current associations and assigns the set of items to the ports.
     */
    def set(items: X*): Unit = {
      ports.clear
      items foreach (_ => add)
      ports zip items foreach {
        case (port, item) => port.set(item)
      }
      info(s"$id.$name set to $items")
    }

    /**
     * Clears the current associations and assigns the set of items to the ports.
     * An alias for `set(items)`.
     */
    def <~(items: X*) = set(items: _*)

    /**
     * Removes the last port in the list.
     */
    def removeLast() = {
      val index = ports.length - 1
      ports.remove(index)
      info(s"$id.$name($index) port removed")
    }

    /**
     * Removes all ports from the list.
     */
    def clear() = {
      ports.clear
      info(s"$id.$name all ports removed")
    }
  }
}

/**
 * A block that has one input.
 */
abstract class RxTransformer[T, R] extends AbstractRxBlock[R] {
  val source = Port[T]("source")

  def from(block: AbstractRxBlock[_ <: T]) = source from block

  def <~(block: AbstractRxBlock[_ <: T]) = from(block)
}

/**
 * A block that has two inputs.
 */
abstract class RxMerger2[T1, T2, R] extends AbstractRxBlock[R] {
  val source1 = Port[T1]("source1")
  val source2 = Port[T2]("source2")

  def from(tuple: (AbstractRxBlock[_ <: T1], AbstractRxBlock[_ <: T2])) = {
    source1 from tuple._1
    source2 from tuple._2
  }

  def <~(tuple: (AbstractRxBlock[_ <: T1], AbstractRxBlock[_ <: T2])) = from(tuple)
}

/**
 * A block that has three inputs.
 */
abstract class RxMerger3[T1, T2, T3, R] extends AbstractRxBlock[R] {
  val source1 = Port[T1]("source1")
  val source2 = Port[T2]("source2")
  val source3 = Port[T3]("source3")

  def from(tuple: (AbstractRxBlock[_ <: T1], AbstractRxBlock[_ <: T2], AbstractRxBlock[_ <: T3])) = {
    source1 from tuple._1
    source2 from tuple._2
    source3 from tuple._3
  }

  def <~(tuple: (AbstractRxBlock[_ <: T1], AbstractRxBlock[_ <: T2], AbstractRxBlock[_ <: T3])) = from(tuple)
}

/**
 * A block that has four inputs.
 */
abstract class RxMerger4[T1, T2, T3, T4, R] extends AbstractRxBlock[R] {
  val source1 = Port[T1]("source1")
  val source2 = Port[T2]("source2")
  val source3 = Port[T3]("source3")
  val source4 = Port[T4]("source4")

  def from(tuple: (AbstractRxBlock[_ <: T1], AbstractRxBlock[_ <: T2], AbstractRxBlock[_ <: T3], AbstractRxBlock[_ <: T4])) = {
    source1 from tuple._1
    source2 from tuple._2
    source3 from tuple._3
    source4 from tuple._4
  }

  def <~(tuple: (AbstractRxBlock[_ <: T1], AbstractRxBlock[_ <: T2], AbstractRxBlock[_ <: T3], AbstractRxBlock[_ <: T4])) = from(tuple)
}

/**
 * A block that has a list of inputs of the same type.
 */
abstract class RxMergerN[T, R] extends AbstractRxBlock[R] {
  val sources = PortList[T]("sources")
}