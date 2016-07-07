package com.ignition.rx.core

import scala.concurrent.duration.Duration

import com.ignition.rx.RxTransformer

class Delay[T] extends RxTransformer[T, T] {
  val period = Port[Duration]("period")

  protected def compute = period.in flatMap source.in.delay
}