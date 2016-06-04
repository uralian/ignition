package com.ignition.rx.core

import rx.lang.scala.Observable
import com.ignition.rx.RxProducer
import scala.collection.mutable.ArrayBuffer

class Sequence[A] extends RxProducer[Seq[A], A](Observable.from[A]) {
  val items = PortList[A]("items")

  protected def combineAttributes = Observable.combineLatest(items map (_.in))(identity)
  protected def inputs: Unit = NO_INPUTS
}