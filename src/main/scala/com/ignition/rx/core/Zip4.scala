package com.ignition.rx.core

import com.ignition.rx.RxMerger4

import rx.lang.scala.Observable

class Zip4[T1, T2, T3, T4] extends RxMerger4[Unit, T1, T2, T3, T4, (T1, T2, T3, T4)](Zip4.evaluate) {
  val source1 = Port[T1]("source1")
  val source2 = Port[T2]("source2")
  val source3 = Port[T3]("source3")
  val source4 = Port[T4]("source4")

  protected def inputs = (source1.in, source2.in, source3.in, source4.in)

  protected def combineAttributes = NO_ATTRIBUTES
}

object Zip4 {
  def evaluate[T1, T2, T3, T4](attrs: Unit) =
    (input1: Observable[T1], input2: Observable[T2], input3: Observable[T3], input4: Observable[T4]) =>
      input1 zip input2 zip input3 zip input4 map {
        case (((i1, i2), i3), i4) => (i1, i2, i3, i4)
      }
}