package com

/**
 * Ignition implicits and helper functions.
 *
 * @author Vlad Orzhekhovskiy
 */
package object ignition {

  /**
   * An extension for Int to be used for connecting an output port of an MultiOutput
   * step with |: notation like this:
   * a|:2 --> 1:|b  // connect 2nd output of a to 1st input of b
   * a|:1 --> b     // connect 1st output of a to the only input of b
   */
  implicit class RichInt(val outIndex: Int) extends AnyVal {
    def -->(inIndex: Int) = OutInIndices(outIndex, inIndex)
    def -->[T](tgtStep: Step[T] with SingleInput[T]) = SInStepOutIndex(outIndex, tgtStep)
  }

  /**
   * An extension of Scala product (a1, a2, ...) object to be used for connecting
   * to the input ports of an MultiInput step.
   *
   * (a, b, c) --> d  // connect the outputs of a, b, and c to the inputs of d
   * (a.out(1), b) --> d // connect output 1 of a and output 0 of b to the inputs of d
   */
  implicit class RichProduct(val product: Product) extends AnyVal {
    def to[T](tgtStep: Step[T] with MultiInput[T]): tgtStep.type = {
      product.productIterator.zipWithIndex foreach {
        case (src: (MultiOutput[_] with Step[_])#OutPort, index) => tgtStep.from(index, src.outer.asInstanceOf[Step[T] with MultiOutput[T]], src.outIndex)
        case (srcStep: Step[T] with SingleOutput[T], index) => tgtStep.from(index, srcStep)
      }
      tgtStep
    }
    def -->[T](tgtStep: Step[T] with MultiInput[T]): tgtStep.type = to(tgtStep)
  }
}