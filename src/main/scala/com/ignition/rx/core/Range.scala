package com.ignition.rx.core

import com.ignition.rx.AbstractRxBlock

import rx.lang.scala.Observable

class Range[A] extends AbstractRxBlock[A] {
  val range = Port[Iterable[A]]("range")

  protected def compute = range.in flatMap Observable.from[A]
}