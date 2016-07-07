package com.ignition.rx.core

import com.ignition.rx.RxTransformer

class TakeWhile[T] extends RxTransformer[T, T] {
  val predicate = Port[T => Boolean]("predicate")

  protected def compute = predicate.in flatMap source.in.takeWhile
}