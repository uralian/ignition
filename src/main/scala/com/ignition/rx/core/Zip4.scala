package com.ignition.rx.core

import com.ignition.rx.RxMerger4

class Zip4[T1, T2, T3, T4] extends RxMerger4[T1, T2, T3, T4, (T1, T2, T3, T4)] {
  protected def compute = source1.in zip source2.in zip source3.in zip source4.in map {
    case (((i1, i2), i3), i4) => (i1, i2, i3, i4)
  }
}