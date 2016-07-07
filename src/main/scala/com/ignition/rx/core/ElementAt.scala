package com.ignition.rx.core

import com.ignition.rx.RxTransformer

class ElementAt[T] extends RxTransformer[T, T] {
  val index = Port[Int]("index")
  val default = Port[Option[T]]("default")
  
  protected def compute = (index.in combineLatest default.in) flatMap {
    case (index, Some(x)) => source.in.elementAtOrDefault(index, x)
    case (index, None) => source.in.elementAt(index)
  }
}