package com.ignition.rx.core

import com.ignition.rx.RxTransformer

class IsEmpty extends RxTransformer[Any, Boolean] {
  protected def compute = source.in.isEmpty
}