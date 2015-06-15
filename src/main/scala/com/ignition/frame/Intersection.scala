package com.ignition.frame

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import com.ignition.SparkRuntime

/**
 * Finds the intersection of the two DataRow RDDs. They must have idential
 * metadata.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Intersection() extends FrameMerger(Intersection.MAX_INPUTS) {

  override val allInputsRequired = false

  protected def compute(args: Seq[DataFrame], limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
    val dfs = args filter (_ != null)
    outSchema
    val result = dfs.tail.foldLeft(dfs.head)((acc: DataFrame, df: DataFrame) => acc.intersect(df))
    optLimit(result, limit)
  }

  protected def computeSchema(inSchemas: Seq[StructType])(implicit runtime: SparkRuntime): StructType = {
    val schemas = inSchemas.filter(_ != null)
    assert(schemas.tail.forall(_ == schemas.head), "Input schemas do not match")
    inSchemas.head
  }
}

/**
 * Intersection companion object.
 */
object Intersection {
  val MAX_INPUTS = 10
}