package com.ignition.workflow.dstream.core

import scala.reflect.ClassTag

import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions

import com.ignition.workflow.dstream.DStreamStep2

class DStreamJoin[K: ClassTag, V: ClassTag, W: ClassTag] extends DStreamStep2[(K, V), (K, W), (K, (V, W))](_ join _)