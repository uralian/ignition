package com.ignition.rx.core

import com.ignition.rx.RxTransformer

class Reduce[T] extends RxTransformer[T, T] {
  val accumulator = Port[(T, T) => T]("accumulator")

  protected def compute = accumulator.in flatMap source.in.reduce
}