package com.ignition.rx.core

import com.ignition.rx.RxTransformer
import rx.lang.scala.Observable

class Collect[T, R] extends RxTransformer[PartialFunction[T, R], T, R](Collect.evaluate) {
  val function = Port[PartialFunction[T, R]]("function")
  val source = Port[T]("source")

  protected def combineAttributes = function.in

  protected def inputs = source.in
}

object Collect {
  def evaluate[T, R](pf: PartialFunction[T, R]) = (input: Observable[T]) => input.collect(pf)
}