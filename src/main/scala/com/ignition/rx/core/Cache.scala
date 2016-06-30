package com.ignition.rx.core

import com.ignition.rx.RxTransformer

class Cache[T] extends RxTransformer[T, T] {
  val capacity = Port[Option[Int]]("capacity")

  protected def compute = capacity.in flatMap {
    case Some(size) => source.in.cacheWithInitialCapacity(size)
    case _          => source.in.cache
  }
}