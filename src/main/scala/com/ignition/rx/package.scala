package com.ignition

/**
 * Types and helper functions for Ignition RX.
 */
package object rx {

  /**
   * Provides additional methods for RX functions.
   */
  implicit class RichValue[T](val x: T) extends AnyVal {

    /**
     * Assigns the value `x` to the specified port.
     */
    def to(port: AbstractRxBlock[_]#Port[_ >: T]) = port.set(x)

    /**
     * Assigns the value `x` to the specified port.
     * Alias for `to(port)`.
     */
    def ~>(port: AbstractRxBlock[_]#Port[_ >: T]) = to(port)
  }

  /**
   * Provides additional methods for block tuples of size 2.
   */
  implicit class RichTuple2[T1, T2](val t: (AbstractRxBlock[T1], AbstractRxBlock[T2])) extends AnyVal {

    /**
     * Connects the blocks to a merger source ports.
     */
    def to[R](target: RxMerger2[_ >: T1, _ >: T2, R]) = {
      target <~ (t._1, t._2)
      target
    }

    /**
     * Connects the blocks to a merger source ports.
     * Alias for `to(block)`.
     */
    def ~>[R](target: RxMerger2[_ >: T1, _ >: T2, R]) = to(target)
  }

  /**
   * Provides additional methods for block tuples of size 3.
   */
  implicit class RichTuple3[T1, T2, T3](val t: (AbstractRxBlock[T1], AbstractRxBlock[T2], AbstractRxBlock[T3])) extends AnyVal {

    /**
     * Connects the blocks to a merger source ports.
     */
    def to[R](target: RxMerger3[_ >: T1, _ >: T2, _ >: T3, R]) = {
      target <~ (t._1, t._2, t._3)
      target
    }

    /**
     * Connects the blocks to a merger source ports.
     * Alias for `to(block)`.
     */
    def ~>[R](target: RxMerger3[_ >: T1, _ >: T2, _ >: T3, R]) = to(target)
  }

  /**
   * Provides additional methods for block tuples of size 4.
   */
  implicit class RichTuple4[T1, T2, T3, T4](val t: (AbstractRxBlock[T1], AbstractRxBlock[T2], AbstractRxBlock[T3], AbstractRxBlock[T4])) extends AnyVal {

    /**
     * Connects the blocks to a merger source ports.
     */
    def to[R](target: RxMerger4[_ >: T1, _ >: T2, _ >: T3, _ >: T4, R]) = {
      target <~ (t._1, t._2, t._3, t._4)
      target
    }

    /**
     * Connects the blocks to a merger source ports.
     * Alias for `to(block)`.
     */
    def ~>[R](target: RxMerger4[_ >: T1, _ >: T2, _ >: T3, _ >: T4, R]) = to(target)
  }
}