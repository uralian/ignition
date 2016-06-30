package com.ignition.rx.core

import com.ignition.rx.RxTransformer

class ZipWithIndex[T] extends RxTransformer[T, (T, Int)] {
  protected def compute = source.in zipWithIndex
}