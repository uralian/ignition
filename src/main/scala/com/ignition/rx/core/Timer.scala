package com.ignition.rx.core

import scala.concurrent.duration.Duration

import com.ignition.rx.RxProducer

import rx.lang.scala.Observable

class Timer extends RxProducer[Duration, Long](Observable.timer) {
  val delay = Port[Duration]("delay")

  protected def combineAttributes = delay.in

  protected def inputs: Unit = NO_INPUTS
}