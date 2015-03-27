package com.ignition

/**
 * Data types, implicits, aliases for DataFrame-based workflows.
 *
 * @author Vlad Orzhekhovskiy
 */
package object flow {

  /**
   * An extension of Scala product (a, b, ...) object to help connect a set of steps
   * to a merger step.
   */
  implicit class RichProduct(val product: Product) extends AnyVal {
    def -->(step: Merger) = {
      product.productIterator.zipWithIndex foreach {
        case (s, index) => step.connectFrom(index, s.asInstanceOf[Step], 0)
      }
      step
    }
  }
}