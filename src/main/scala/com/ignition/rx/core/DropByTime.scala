package com.ignition.rx.core

import scala.concurrent.duration.Duration

import com.ignition.rx.RxTransformer

import rx.lang.scala.Observable

class DropByTime[T] extends RxTransformer[Duration, T, T](DropByTime.evaluate) {
  val period = Port[Duration]("period")
  val source = Port[T]("source")

  protected def combineAttributes = period.in

  protected def inputs = source.in
}

object DropByTime {
  def evaluate[T](period: Duration) = (input: Observable[T]) => input drop period
}
