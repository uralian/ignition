package com.ignition.rx.core

import com.ignition.rx.RxTransformer

class Length extends RxTransformer[Any, Int] {
  protected def compute = source.in.length
}