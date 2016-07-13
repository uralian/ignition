package com.ignition.rx.core

import com.ignition.rx.RxMergerN

import rx.lang.scala.Observable

class CombineLatest[T] extends RxMergerN[T, Seq[T]] {
  protected def compute = Observable.combineLatest(sources.ins.toIterable)(identity)
}