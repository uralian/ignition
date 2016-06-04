package com.ignition.rx.core

import scala.concurrent.duration.Duration

import com.ignition.rx.RxProducer

import rx.lang.scala.Observable

class Interval extends RxProducer[(Duration, Duration), Long](Interval.evaluate) {
  val initial = Port[Duration]("initial")
  val period = Port[Duration]("period")

  protected def combineAttributes = initial.in combineLatest period.in

  protected def inputs: Unit = NO_INPUTS
}

object Interval {
  def evaluate(attrs: (Duration, Duration)) = Observable.interval(attrs._1, attrs._2)
}