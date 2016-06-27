package com.ignition.rx.core

import com.ignition.rx.RxMergerN

import rx.lang.scala.Observable

class CombineLatest[T] extends RxMergerN[Unit, T, Seq[T]](CombineLatest.evaluate) {
  val sources = PortList[T]("sources")

  protected def combineAttributes = NO_ATTRIBUTES

  protected def inputs = sources map (_.in)
}

object CombineLatest {
  def evaluate[T](attrs: Unit) = (inputs: Seq[Observable[T]]) => Observable.combineLatest(inputs)(identity)
}