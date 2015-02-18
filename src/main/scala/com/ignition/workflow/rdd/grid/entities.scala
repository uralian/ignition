package com.ignition.workflow.rdd.grid

import scala.xml.{ Elem, Node }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.ignition.data.{ DataRow, RowMetaData }
import com.ignition.workflow.Step0
import com.ignition.workflow.Step1

/**
 * The base step of the ignition grid framework.
 *
 * @author Vlad Orzhekhovskiy
 */
trait GridStep {

  /**
   * Returns the output metadata of the step.
   */
  def outMetaData: Option[RowMetaData]

  /**
   * Converts this step into XML.
   */
  def toXml: Elem

  /* spark helpers */

  protected def defaultPartitions(implicit sc: SparkContext) = sc.defaultMinPartitions

  protected def defaultParallelism(implicit sc: SparkContext) = sc.defaultParallelism
}

/**
 * Grid step without inputs.
 */
trait GridStep0 extends Step0[RDD[DataRow], SparkContext] with GridStep {

  protected def computeRDD(implicit sc: SparkContext): RDD[DataRow]

  protected def compute(sc: SparkContext): RDD[DataRow] = computeRDD(sc)
}

/**
 * Grid step with one input.
 */
trait GridStep1 extends Step1[RDD[DataRow], RDD[DataRow], SparkContext] with GridStep {

  protected def computeRDD(rdd: RDD[DataRow]): RDD[DataRow]

  protected def compute(sc: SparkContext)(rdd: RDD[DataRow]): RDD[DataRow] = computeRDD(rdd)

  protected def inMetaData: Option[RowMetaData] = in flatMap {
    _.asInstanceOf[GridStep].outMetaData
  }
}

/**
 * Extended by factories that can restore instances from XML.
 */
trait XmlFactory[T] {
  /**
   * Creates an object instance from XML.
   */
  def fromXml(xml: Node): T
}