package com.ignition.rx.core

import com.ignition.rx.RxTransformer

class Retry[T] extends RxTransformer[T, T] {
  val count = Port[Option[Long]]("count")

  protected def compute = count.in flatMap {
    case None    => source.in.retry
    case Some(n) => source.in.retry(n)
  }
}