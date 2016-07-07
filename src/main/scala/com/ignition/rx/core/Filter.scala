package com.ignition.rx.core

import com.ignition.rx.RxTransformer

class Filter[T] extends RxTransformer[T, T] {
  val predicate = Port[T => Boolean]("predicate")

  protected def compute = predicate.in flatMap source.in.filter
}