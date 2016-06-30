package com.ignition.rx.core

import com.ignition.rx.AbstractRxBlock

import rx.lang.scala.Observable

class Sequence[A] extends AbstractRxBlock[A] {
  val items = PortList[A]("items")

  protected def compute = Observable.combineLatest(items.ins)(identity) flatMap Observable.from[A]
}