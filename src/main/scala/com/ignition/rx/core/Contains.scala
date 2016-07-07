package com.ignition.rx.core

import com.ignition.rx.RxTransformer

class Contains[T] extends RxTransformer[T, Boolean] {
  val item = Port[T]("item")
 
  protected def compute = item.in flatMap source.in.contains
}