package com.ignition.rx.core

import scala.concurrent.duration.Duration

import com.ignition.rx.AbstractRxBlock

import rx.lang.scala.Observable

class Interval extends AbstractRxBlock[Long] {
  val initial = Port[Duration]("initial")
  val period = Port[Duration]("period")

  protected def compute = (initial.in combineLatest period.in) flatMap {
    case (i, p) => Observable.interval(i, p)
  }
}