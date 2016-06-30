package com.ignition.rx.core

import com.ignition.rx.RxTransformer

class Distinct[T] extends RxTransformer[T, T] {
  val selector = Port[T => _]("selector")
  val global = Port[Boolean]("global")

  protected def combineAttributes = selector.in combineLatest global.in

  protected def inputs = source.in

  protected def compute = (selector.in combineLatest global.in) flatMap {
    case (func, true)  => source.in distinct func
    case (func, false) => source.in distinctUntilChanged func
  }
}