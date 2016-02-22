package com.ignition.frame

import scala.xml.{ Elem, Node }

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.json4s.JValue
import org.json4s.JsonDSL.{ pair2jvalue, string2jvalue }

/**
 * Finds the intersection of the two DataRow RDDs. They must have idential
 * metadata.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Intersection() extends FrameMerger(Intersection.MAX_INPUTS) {
  import Intersection._

  override val allInputsRequired = false

  protected def compute(args: IndexedSeq[DataFrame])(implicit runtime: SparkRuntime): DataFrame = {
    validateSchema
    val dfs = args filter (_ != null)
    val result = dfs.tail.foldLeft(dfs.head)((acc: DataFrame, df: DataFrame) => acc.intersect(df))
    optLimit(result, runtime.previewMode)
  }

  override protected def buildSchema(index: Int)(implicit runtime: SparkRuntime): StructType = validateSchema

  private def inSchemas(implicit runtime: SparkRuntime) = inputs filter (_ != null) map (_.schema)

  private def validateSchema(implicit runtime: SparkRuntime): StructType = {
    assert(!inSchemas.isEmpty, "No inputs connected")
    assert(inSchemas.tail.forall(_ == inSchemas.head), "Input schemas do not match")
    inSchemas.head
  }

  def toXml: Elem = <node/>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag)
}

/**
 * Intersection companion object.
 */
object Intersection {
  val tag = "intersection"

  val MAX_INPUTS = 10

  def fromXml(xml: Node) = apply()

  def fromJson(json: JValue) = apply()
}