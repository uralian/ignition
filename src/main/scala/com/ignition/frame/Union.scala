package com.ignition.frame

import scala.xml.{ Elem, Node }

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.json4s.JValue
import org.json4s.JsonDSL.{ pair2jvalue, string2jvalue }

import com.ignition.SparkRuntime

/**
 * Merges multiple DataFrames. All of them must have identical schema.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Union() extends FrameMerger(Union.MAX_INPUTS) {
  import Union._

  override val allInputsRequired = false

  protected def compute(args: IndexedSeq[DataFrame], preview: Boolean)(implicit runtime: SparkRuntime): DataFrame = {
    validateSchema
    val rdd = ctx.sparkContext.union(args filter (_ != null) map (_.rdd))
    val df = ctx.createDataFrame(rdd, inSchemas.head)
    optLimit(df, preview)
  }

  override protected def buildSchema(index: Int)(implicit runtime: SparkRuntime): StructType = validateSchema

  private def inSchemas(implicit runtime: SparkRuntime) = inputs(true) filter (_ != null) map (_.schema)

  private def validateSchema(implicit runtime: SparkRuntime): StructType = {
    assert(!inSchemas.isEmpty, "No inputs connected")
    assert(inSchemas.tail.forall(_ == inSchemas.head), "Input schemas do not match")
    inSchemas.head
  }

  def toXml: Elem = <node/>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag)
}

/**
 * Union companion object.
 */
object Union {
  val tag = "union"

  val MAX_INPUTS = 10

  def fromXml(xml: Node) = apply()

  def fromJson(json: JValue) = apply()
}