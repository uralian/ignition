package com.ignition.rx.core

import scala.concurrent.duration.Duration

import com.ignition.rx.RxTransformer

class Debounce[T] extends RxTransformer[T, T] {
  val timeout = Port[Duration]("timeout")
  
  protected def compute = timeout.in flatMap source.in.debounce
}