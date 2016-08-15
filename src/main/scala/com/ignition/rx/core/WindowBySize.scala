package com.ignition.rx.core

import com.ignition.rx.RxTransformer

class WindowBySize[T] extends RxTransformer[T, Seq[T]] {
  val count = Port[Int]("count")
  val skip = Port[Int]("skip")

  protected def compute = (count.in combineLatest skip.in) flatMap {
    case (cnt, skp) if cnt == skp => source.in.tumblingBuffer(cnt)
    case (cnt, skp)               => source.in.slidingBuffer(cnt, skp)
  }
}