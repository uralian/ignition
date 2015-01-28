package com.ignition.workflow.rdd.basic

import scala.reflect.ClassTag

import com.ignition.workflow.rdd.RDDStep0

class RDDTextFile(path: String) extends RDDStep0[String](_ textFile path)

class RDDTextFolder(path: String) extends RDDStep0[(String, String)](_ wholeTextFiles path)

class RDDSequence[T: ClassTag](seq: Seq[T]) extends RDDStep0[T](_ parallelize seq)
