package com.ignition.rx.core

import com.ignition.rx.RxProducer

import rx.lang.scala.Observable

class Dummy extends RxProducer[Unit, Boolean](Dummy.evaluate) {
  protected def combineAttributes = NO_ATTRIBUTES
  protected def inputs = NO_INPUTS
}

object Dummy {
  def evaluate(a: Unit) = Observable.just(true)
}