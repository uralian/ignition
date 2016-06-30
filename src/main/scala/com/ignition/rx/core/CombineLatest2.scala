package com.ignition.rx.core

import com.ignition.rx.RxMerger2

class CombineLatest2[T1, T2] extends RxMerger2[T1, T2, (T1, T2)] {
  protected def compute = source1.in combineLatest source2.in
}