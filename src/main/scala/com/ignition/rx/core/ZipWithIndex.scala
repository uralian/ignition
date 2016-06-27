package com.ignition.rx.core

import com.ignition.rx.RxTransformer

import rx.lang.scala.Observable

class ZipWithIndex[T] extends RxTransformer[Unit, T, (T, Int)](ZipWithIndex.evaluate) {
  val source = Port[T]("source")

  protected def combineAttributes = NO_ATTRIBUTES

  protected def inputs = source.in
}

object ZipWithIndex {
  def evaluate[T](attrs: Unit) = (input: Observable[T]) => input.zipWithIndex
}