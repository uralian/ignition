package com.ignition.rx.core

import com.ignition.rx.RxMerger2

class Concat[T] extends RxMerger2[T, T, T] {
  protected def compute = source1.in ++ source2.in
}