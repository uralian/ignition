package com.ignition.rx.core

import scala.concurrent.duration.Duration

import com.ignition.rx.AbstractRxBlock

import rx.lang.scala.Observable

class Timer extends AbstractRxBlock[Long] {
  val delay = Port[Duration]("delay")

  protected def compute = delay.in flatMap Observable.timer
}