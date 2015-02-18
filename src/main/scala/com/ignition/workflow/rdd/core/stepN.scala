package com.ignition.workflow.rdd.core

import scala.reflect.ClassTag

import org.apache.spark.SparkContext._

import com.ignition.workflow.rdd.RDDStepN

class RDDUnion[T: ClassTag] extends RDDStepN[T, T](sc => rdds => sc.union(rdds.toSeq))