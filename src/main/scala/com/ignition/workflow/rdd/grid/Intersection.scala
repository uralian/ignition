package com.ignition.workflow.rdd.grid

import scala.xml.{ Elem, Node }

import org.apache.spark.rdd.RDD

import com.ignition.data.{ DataRow, RowMetaData }
import com.ignition.util.XmlUtils.{ RichNodeSeq, intToText, optToOptText }
import com.ignition.workflow.WorkflowException

/**
 * Finds the intersection of the two DataRow RDDs. They must have idential
 * metadata.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Intersection(numPartitions: Option[Int] = None) extends GridStep2 {

  protected def computeRDD(rdd1: RDD[DataRow], rdd2: RDD[DataRow]): RDD[DataRow] = {
    assert(outMetaData.isDefined, "Input1 or Input2 is not connected")

    implicit val sc = rdd1.sparkContext
    rdd1 intersection (rdd2, numPartitions getOrElse defaultParallelism)
  }

  def outMetaData: Option[RowMetaData] = inMetaData map {
    case (meta1, meta2) if meta1 == meta2 => meta1
    case _ => throw WorkflowException("Input1 and Input2 metadata do not match")
  }

  def toXml: Elem = <intersection partitions={ numPartitions }/>
  
  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}

/**
 * Intersection companion object.
 */
object Intersection extends XmlFactory[Intersection] {
  def fromXml(xml: Node) = {
    val numPartitions = (xml \ "@partitions").getAsInt
    Intersection(numPartitions)
  }
}