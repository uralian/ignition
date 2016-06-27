package com.ignition.rx.core

import com.ignition.rx.RxMerger2

import rx.lang.scala.Observable

class Zip2[T1, T2] extends RxMerger2[Unit, T1, T2, (T1, T2)](Zip2.evaluate) {
  val source1 = Port[T1]("source1")
  val source2 = Port[T2]("source2")

  protected def combineAttributes = NO_ATTRIBUTES

  protected def inputs = (source1.in, source2.in)
}

object Zip2 {
  def evaluate[T1, T2](attrs: Unit) =
    (input1: Observable[T1], input2: Observable[T2]) => input1 zip input2
}