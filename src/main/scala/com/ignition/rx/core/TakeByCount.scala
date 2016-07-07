package com.ignition.rx.core

import com.ignition.rx.RxTransformer

class TakeByCount[T] extends RxTransformer[T, T] {
  val count = Port[Int]("count")

  protected def compute = count.in flatMap source.in.take
}