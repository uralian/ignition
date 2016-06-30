package com.ignition.rx.core

import com.ignition.rx.RxTransformer

class Collect[T, R] extends RxTransformer[T, R] {
  val selector = Port[PartialFunction[T, R]]("selector")

  protected def compute = selector.in flatMap source.in.collect
}