package com.ignition.rx.core

import com.ignition.rx.RxMergerN

import rx.lang.scala.Observable

class Zip[T] extends RxMergerN[Unit, T, Seq[T]](Zip.evaluate) {
  val sources = PortList[T]("sources")

  protected def combineAttributes = NO_ATTRIBUTES

  protected def inputs = sources map (_.in)
}

object Zip {
  def evaluate[T](attrs: Unit) = (inputs: Seq[Observable[T]]) => Observable.zip(Observable.from(inputs))
}