package com.ignition.rx.core

import com.ignition.rx.RxTransformer

class Last[T] extends RxTransformer[T, T] {
  val default = Port[Option[T]]("default")

  protected def compute = default.in flatMap {
    case Some(x) => source.in.lastOrElse(x)
    case None    => source.in.last
  }
}