package com.ignition.rx.core

import com.ignition.rx.RxMerger2

class Merge[T] extends RxMerger2[T, T, T] {
  protected def compute = source1.in merge source2.in
}