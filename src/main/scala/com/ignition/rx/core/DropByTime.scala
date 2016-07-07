package com.ignition.rx.core

import scala.concurrent.duration.Duration

import com.ignition.rx.RxTransformer

class DropByTime[T](right: Boolean) extends RxTransformer[T, T] {
  val period = Port[Duration]("period")

  protected def compute = right match {
    case true  => period.in flatMap source.in.dropRight
    case false => period.in flatMap source.in.drop
  }
}