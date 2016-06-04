package com.ignition.rx.core

import com.ignition.rx.RxProducer

import rx.lang.scala.Observable

class ValueHolder[A] extends RxProducer[A, A](ValueHolder.evaluate) {
  val value = Port[A]("value")

  protected def combineAttributes = value.in
  protected def inputs: Unit = NO_INPUTS
}

object ValueHolder {
  def evaluate[A](value: A) = Observable.just(value)
}