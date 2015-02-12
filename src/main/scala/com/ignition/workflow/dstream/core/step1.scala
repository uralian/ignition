package com.ignition.workflow.dstream.core

import scala.reflect.ClassTag

import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions

import com.ignition.workflow.dstream.DStreamStep1

class DStreamMap[S: ClassTag, T: ClassTag](func: S => T) extends DStreamStep1[S, T](_ map func)

class DStreamFlatMap[S: ClassTag, T: ClassTag](func: S => Traversable[T]) extends DStreamStep1[S, T](_ flatMap func)

class DStreamFilter[T: ClassTag](func: T => Boolean) extends DStreamStep1[T, T](_ filter func)

class DStreamReduceByKey[K: ClassTag, V: ClassTag](func: (V, V) => V)
  extends DStreamStep1[(K, V), (K, V)](_ reduceByKey func)