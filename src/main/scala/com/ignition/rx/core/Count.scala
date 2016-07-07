package com.ignition.rx.core

import com.ignition.rx.RxTransformer

class Count[T] extends RxTransformer[T, Int] {
  val predicate = Port[T => Boolean]("predicate")

  protected def compute = predicate.in flatMap source.in.count
}