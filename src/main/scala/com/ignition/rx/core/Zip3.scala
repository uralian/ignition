package com.ignition.rx.core

import com.ignition.rx.RxMerger3

import rx.lang.scala.Observable

class Zip3[T1, T2, T3] extends RxMerger3[Unit, T1, T2, T3, (T1, T2, T3)](Zip3.evaluate) {
  val source1 = Port[T1]("source1")
  val source2 = Port[T2]("source2")
  val source3 = Port[T3]("source3")

  protected def inputs = (source1.in, source2.in, source3.in)

  protected def combineAttributes = NO_ATTRIBUTES
}

object Zip3 {
  def evaluate[T1, T2, T3](attrs: Unit) =
    (input1: Observable[T1], input2: Observable[T2], input3: Observable[T3]) =>
      input1 zip input2 zip input3 map {
        case ((i1, i2), i3) => (i1, i2, i3)
      }
}