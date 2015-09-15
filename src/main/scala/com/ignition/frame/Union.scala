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

  protected def compute(args: Seq[DataFrame], limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
    val rdd = ctx.sparkContext.union(args filter (_ != null) map (_.rdd))
    val df = ctx.createDataFrame(rdd, outSchema(0))
    optLimit(df, limit)
  }

  protected def computeSchema(inSchemas: Seq[StructType])(implicit runtime: SparkRuntime): StructType = {
    val schemas = inSchemas.filter(_ != null)
    assert(schemas.tail.forall(_ == schemas.head), "Input schemas do not match")
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