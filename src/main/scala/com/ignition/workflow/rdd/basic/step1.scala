package com.ignition.workflow.rdd.basic

import scala.reflect.ClassTag

import org.apache.spark.SparkContext._

import com.ignition.workflow.rdd.RDDStep1

class RDDMap[S: ClassTag, T: ClassTag](func: S => T) extends RDDStep1[S, T](_ map func)

class RDDFlatMap[S: ClassTag, T: ClassTag](func: S => TraversableOnce[T]) extends RDDStep1[S, T](_ flatMap func)

class RDDFilter[T: ClassTag](func: T => Boolean) extends RDDStep1[T, T](_ filter func)

class RDDReduceByKey[K: ClassTag, V: ClassTag](func: (V, V) => V)
  extends RDDStep1[(K, V), (K, V)](_ reduceByKey func)