package com.ignition.rx.numeric

import com.ignition.rx.RxTransformer

class Sum[T](implicit num: Numeric[T]) extends RxTransformer[T, T] {
  protected def compute = source.in.sum
}