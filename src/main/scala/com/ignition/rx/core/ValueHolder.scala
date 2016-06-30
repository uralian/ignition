package com.ignition.rx.core

import com.ignition.rx.AbstractRxBlock

import rx.lang.scala.Observable

class ValueHolder[A] extends AbstractRxBlock[A] {
  val value = Port[A]("value")

  protected def compute = value.in flatMap (Observable.just(_))
}