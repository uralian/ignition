package com.ignition.rx.core

import com.ignition.rx.AbstractRxBlock

import rx.lang.scala.Observable

class Zero[T](implicit num: Numeric[T]) extends AbstractRxBlock[T] {
  protected def compute = Observable.just(num.zero)
}