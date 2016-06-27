package com.ignition.rx.core

import com.ignition.rx.RxTransformer

import rx.lang.scala.Observable

class DropByCount[T] extends RxTransformer[Int, T, T](DropByCount.evaluate) {
  val count = Port[Int]("count")
  val source = Port[T]("source")

  protected def combineAttributes = count.in

  protected def inputs = source.in
}

object DropByCount {
  def evaluate[T](count: Int) = (input: Observable[T]) => input drop count
}
