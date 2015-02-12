package com.ignition.workflow.rdd.core

import scala.reflect.ClassTag

import org.apache.spark.SparkContext._

import com.ignition.workflow.rdd.RDDStep2

class RDDIntersection[T: ClassTag] extends RDDStep2[T, T, T](_ intersection _)

class RDDCartesian[S1: ClassTag, S2: ClassTag] extends RDDStep2[S1, S2, (S1, S2)](_ cartesian _)

class RDDJoin[K: ClassTag, V: ClassTag, W: ClassTag]
  extends RDDStep2[(K, V), (K, W), (K, (V, W))](_ join _)