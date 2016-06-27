package com.ignition.rx.core

import com.ignition.rx.RxTransformer

import rx.lang.scala.Observable

class Distinct[T] extends RxTransformer[(T => _, Boolean), T, T](Distinct.evaluate) {
  val selector = Port[T => _]("selector")
  val global = Port[Boolean]("global")
  val source = Port[T]("source")

  protected def combineAttributes = selector.in combineLatest global.in

  protected def inputs = source.in
}

object Distinct {
  def evaluate[T](attrs: (T => _, Boolean)) = {
    val (selector, global) = attrs
    global match {
      case true => (input: Observable[T]) => input distinct selector
      case false => (input: Observable[T]) => input distinctUntilChanged selector
    }
  }
}