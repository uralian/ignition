package com.ignition.rx.core

import com.ignition.rx.RxProducer

import rx.lang.scala.Observable

class Range[A] extends RxProducer[Iterable[A], A](Observable.from[A]) {
  val range = Port[Iterable[A]]("range")

  protected def combineAttributes = range.in
  protected def inputs: Unit = NO_INPUTS
}