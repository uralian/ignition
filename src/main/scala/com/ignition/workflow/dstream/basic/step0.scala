package com.ignition.workflow.dstream.basic

import scala.collection.mutable.Queue
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

import com.ignition.workflow.dstream.DStreamStep0

class DStreamTextFiles(dir: String) extends DStreamStep0[String](_ textFileStream dir)

class DStreamTextSocket(hostname: String, port: Int) extends DStreamStep0[String](_ socketTextStream (hostname, port))

class DStreamQueue[T: ClassTag](queue: Queue[RDD[T]], oneAtATime: Boolean) extends DStreamStep0[T](_ queueStream (queue, oneAtATime))