package com.ignition.rx.core

import com.ignition.rx.RxTransformer

class Exists[T] extends RxTransformer[T, Boolean] {
  val predicate = Port[T => Boolean]("predicate")

  protected def compute = predicate.in flatMap source.in.exists
}