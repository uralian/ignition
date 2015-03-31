package com.ignition

/**
 * Data types, implicits, aliases for DataFrame-based workflows.
 *
 * @author Vlad Orzhekhovskiy
 */
package object flow {
  
  /**
   * An extension for Int to be used for connecting an output port of an MultiOutput
   * step with |: notation like this:
   * a|:2 --> 1:|b  // connect 2nd output of a to 1st input of b
   * a|:1 --> b     // connect 1st output of a to the only input of b
   */
  implicit class RichInt(val outIndex: Int) extends AnyVal {
    def -->(inIndex: Int) = OutInIndices(outIndex, inIndex)
    def -->(tgtStep: Step with SingleInput) = SInStepOutIndex(outIndex, tgtStep)
  }

  /**
   * An extension of Scala product (a1, a2, ...) object to be used for connecting
   * to the input ports of an MultiInput step.
   * 
   * (a, b, c) --> d  // connect the outputs of a, b, and c to the inputs of d
   */
  implicit class RichProduct(val product: Product) extends AnyVal {
    def to(tgtStep: Step with MultiInput): tgtStep.type = {
      product.productIterator.zipWithIndex foreach {
        case (srcStep: Step with SingleOutput, index) => tgtStep.from(index, srcStep)
      }
      tgtStep
    }
    def -->(tgtStep: Step with MultiInput): tgtStep.type = to(tgtStep)
  }
}