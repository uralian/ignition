package com.ignition.workflow.rdd.grid

import scala.xml.{ Elem, Node }

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.ignition.data.{ DataRow, RowMetaData }

/**
 * Merges multiple DataRow RDDs. All of them must have identical metadata.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Union() extends GridStepN {

  protected def computeRDD(rdds: Iterable[RDD[DataRow]])(implicit sc: SparkContext): RDD[DataRow] = {
    outMetaData
    sc union rdds.toSeq
  }

  def outMetaData: Option[RowMetaData] = inMetaData

  def toXml: Elem = <union/>

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}

/**
 * Union companion object.
 */
object Union extends XmlFactory[Union] {
  def fromXml(xml: Node) = Union()
}