package com.ignition.rx.core

import com.ignition.rx.RxMergerN

import rx.lang.scala.Observable

class AMB[T] extends RxMergerN[T, T] {
  protected def compute = Observable.amb(sources.ins: _*)
}