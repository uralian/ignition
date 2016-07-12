package com.ignition.rx.numeric

import com.ignition.rx.RxTransformer

class Min[T](implicit num: Numeric[T]) extends RxTransformer[T, T] {
  protected def compute = source.in.foldLeft[Option[T]](None) {
    case (Some(y), x) => Some(num.min(y, x))
    case (_, x)       => Some(x)
  } map (_ getOrElse (throw new RuntimeException("No values found")))
}