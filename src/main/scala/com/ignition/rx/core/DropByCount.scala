package com.ignition.rx.core

import com.ignition.rx.RxTransformer

class DropByCount[T](right: Boolean) extends RxTransformer[T, T] {
  val count = Port[Int]("count")

  protected def compute = right match {
    case true => count.in flatMap source.in.dropRight
    case false => count.in flatMap source.in.drop
  }
}