package com.ignition.rx.core

import com.ignition.rx.RxMergerN

import rx.lang.scala.Observable

class Zip[T] extends RxMergerN[T, Seq[T]] {
  protected def compute = Observable.zip(Observable.from(sources.ins))
}