package com.ignition.workflow.dstream.core

import scala.reflect.ClassTag

import com.ignition.workflow.dstream.DStreamStepN

class DStreamUnion[T: ClassTag] extends DStreamStepN[T, T](sc => DStreams => sc.union(DStreams.toSeq))