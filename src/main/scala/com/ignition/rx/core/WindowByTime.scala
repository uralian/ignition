package com.ignition.rx.core

import scala.concurrent.duration.Duration

import com.ignition.rx.RxTransformer

class WindowByTime[T] extends RxTransformer[T, Seq[T]] {
  val span = Port[Duration]("span")
  val shift = Port[Duration]("shift")

  protected def compute = (span.in combineLatest shift.in) flatMap {
    case (sp, sh) if sp == sh => source.in.tumblingBuffer(sp)
    case (sp, sh)             => source.in.slidingBuffer(sp, sh)
  }
}