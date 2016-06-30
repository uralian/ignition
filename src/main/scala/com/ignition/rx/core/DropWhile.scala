package com.ignition.rx.core

import com.ignition.rx.RxTransformer

class DropWhile[T] extends RxTransformer[T, T] {
  val predicate = Port[T => Boolean]("predicate")

  protected def compute = predicate.in flatMap source.in.dropWhile
}