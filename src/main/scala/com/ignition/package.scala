package com

/**
 * Ignition implicits and helper functions.
 *
 * @author Vlad Orzhekhovskiy
 */
package object ignition {

  val STEPS_SERIALIZABLE = "step.serializable"

  private type CSrc[T] = ConnectionSource[T]

  /* implicits for connecting tuples of ConnectionSource to a multi-input step */

  implicit class CSource2[T](val tuple: Product2[CSrc[T], CSrc[T]]) extends AnyVal {
    def to(tgt: MultiInputStep[T]): tgt.type = {
      tuple._1 to tgt.in(0)
      tuple._2 to tgt.in(1)
      tgt
    }
    def -->(tgt: MultiInputStep[T]): tgt.type = to(tgt)
  }

  implicit class CSource3[T](val tuple: Product3[CSrc[T], CSrc[T], CSrc[T]]) extends AnyVal {
    def to(tgt: MultiInputStep[T]): tgt.type = {
      (tuple._1, tuple._2) to tgt
      tuple._3 to tgt.in(2)
      tgt
    }
    def -->(tgt: MultiInputStep[T]): tgt.type = to(tgt)
  }

  implicit class CSource4[T](val tuple: Product4[CSrc[T], CSrc[T], CSrc[T], CSrc[T]]) extends AnyVal {
    def to(tgt: MultiInputStep[T]): tgt.type = {
      (tuple._1, tuple._2, tuple._3) to tgt
      tuple._4 to tgt.in(3)
      tgt
    }
    def -->(tgt: MultiInputStep[T]): tgt.type = to(tgt)
  }

  implicit class CSource5[T](val tuple: Product5[CSrc[T], CSrc[T], CSrc[T], CSrc[T], CSrc[T]]) extends AnyVal {
    def to(tgt: MultiInputStep[T]): tgt.type = {
      (tuple._1, tuple._2, tuple._3, tuple._4) to tgt
      tuple._5 to tgt.in(4)
      tgt
    }
    def -->(tgt: MultiInputStep[T]): tgt.type = to(tgt)
  }

  /**
   * Converts a single value to a Product of size 1 for consistency in some API calls.
   */
  implicit def value2tuple[T](x: T): Tuple1[T] = Tuple1(x)

  /**
   * Returns the outbound ports of a step.
   * Having this private convenience function rather than making Step more generic and
   * less type safe.
   */
  private[ignition] def outs[T](step: Step[T]): Seq[ConnectionSource[T]] = step match {
    case x if x.isInstanceOf[SingleOutputStep[T]] => List(x.asInstanceOf[SingleOutputStep[T]])
    case x if x.isInstanceOf[MultiOutputStep[T]] => x.asInstanceOf[MultiOutputStep[T]].out
    case _ => Nil
  }

  /**
   * Returns the inbounds ports of a step.
   * Having this private convenience function rather than making Step more generic and
   * less type safe.
   */
  private[ignition] def ins[T](step: Step[T]): Seq[ConnectionTarget[T]] = step match {
    case x if x.isInstanceOf[SingleInputStep[T]] => List(x.asInstanceOf[SingleInputStep[T]])
    case x if x.isInstanceOf[MultiInputStep[T]] => x.asInstanceOf[MultiInputStep[T]].in
    case _ => Nil
  }
}