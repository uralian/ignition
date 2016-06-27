package com.ignition.rx.core

import scala.concurrent.duration.Duration

import com.ignition.rx.RxTransformer

import rx.lang.scala.Observable

class Delay[T] extends RxTransformer[Duration, T, T](Delay.evaluate) {
  val period = Port[Duration]("period")
  val source = Port[T]("source")

  protected def combineAttributes = period.in

  protected def inputs = source.in
}

object Delay {
  def evaluate[T](period: Duration) = (input: Observable[T]) => input delay period
}
