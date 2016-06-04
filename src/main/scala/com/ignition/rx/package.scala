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
    def ~>(port: AbstractRxBlock[_, _, _]#Port[_ >: T]) = port.set(x)
  }
}