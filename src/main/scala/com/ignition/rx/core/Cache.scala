package com.ignition.rx.core

import com.ignition.rx.RxTransformer
import rx.lang.scala.Observable

class Cache[T] extends RxTransformer[Option[Int], T, T](Cache.evaluate) {
  val capacity = Port[Option[Int]]("capacity")
  val source = Port[T]("source")

  protected def combineAttributes = capacity.in

  protected def inputs = source.in
}

object Cache {
  def evaluate[T](capacity: Option[Int]) = capacity match {
    case Some(size) => (input: Observable[T]) => input.cacheWithInitialCapacity(size)
    case _ => (input: Observable[T]) => input.cache
  }
}