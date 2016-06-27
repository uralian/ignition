package com.ignition.rx.core

import com.ignition.rx.RxMergerN

import rx.lang.scala.Observable

class AMB[T] extends RxMergerN[Unit, T, T](AMB.evaluate) {
  val sources = PortList[T]("sources")

  protected def combineAttributes = NO_ATTRIBUTES

  protected def inputs = sources map (_.in)
}

object AMB {
  def evaluate[T](attrs: Unit) = (inputs: Seq[Observable[T]]) => Observable.amb(inputs: _*)
}