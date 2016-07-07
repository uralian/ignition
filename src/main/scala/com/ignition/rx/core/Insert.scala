package com.ignition.rx.core

import com.ignition.rx.RxTransformer

class Insert[T](prepend: Boolean) extends RxTransformer[T, T] {
  val item = Port[T]("item")

  protected def compute = prepend match {
    case true  => item.in flatMap (_ +: source.in)
    case false => item.in flatMap (source.in :+ _)
  }
}