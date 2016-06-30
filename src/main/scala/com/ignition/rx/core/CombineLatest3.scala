package com.ignition.rx.core

import com.ignition.rx.RxMerger3

class CombineLatest3[T1, T2, T3] extends RxMerger3[T1, T2, T3, (T1, T2, T3)] {
  protected def compute = source1.in combineLatest source2.in combineLatest source3.in map {
    case ((i1, i2), i3) => (i1, i2, i3)
  }
}