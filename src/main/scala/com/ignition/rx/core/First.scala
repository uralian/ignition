package com.ignition.rx.core

import com.ignition.rx.RxTransformer

class First[T] extends RxTransformer[T, T] {
  val default = Port[Option[T]]("default")

  protected def compute = default.in flatMap {
    case Some(x) => source.in.firstOrElse(x)
    case None    => source.in.first
  }
}