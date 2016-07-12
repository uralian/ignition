package com.ignition.rx.core

import com.ignition.rx.RxTransformer

class Scan[T, R] extends RxTransformer[T, R] {
  val initial = Port[R]("initial")
  val accumulator = Port[(R, T) => R]("accumulator")

  protected def compute = (initial.in combineLatest accumulator.in) flatMap {
    case (init, acc) => source.in.scan(init)(acc)
  }
}