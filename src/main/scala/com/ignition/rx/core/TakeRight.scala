package com.ignition.rx.core

import scala.concurrent.duration.Duration

import com.ignition.rx.RxTransformer

class TakeRight[T] extends RxTransformer[T, T] {
  val period = Port[Option[Duration]]("period")
  val count = Port[Option[Int]]("count")

  protected def compute = period.in combineLatest count.in flatMap {
    case (Some(time), Some(num)) => source.in takeRight (num, time)
    case (Some(time), None)      => source.in takeRight time
    case (None, Some(num))       => source.in takeRight num
    case _                       => throw new IllegalArgumentException("Neither period nor count set")
  }
}