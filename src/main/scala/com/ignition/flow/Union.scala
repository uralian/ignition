package com.ignition.flow

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import com.ignition.SparkRuntime

/**
 * Merges multiple DataFrames. All of them must have identical schema.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Union() extends Merger(Union.MAX_INPUTS) {
  
  override val allInputsRequired = false

  protected def compute(args: Array[DataFrame], limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
    val rdd = ctx.sparkContext.union(args filter (_ != null) map (_.rdd))
    val df = ctx.createDataFrame(rdd, outSchema(0))
    optLimit(df, limit)
  }

  protected def computeSchema(inSchemas: Array[StructType])(implicit runtime: SparkRuntime): StructType = {
    val schemas = inSchemas.filter(_ != null)
    assert(schemas.tail.forall(_ == schemas.head), "Input schemas do not match")
    inSchemas.head
  }
  
  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}

/**
 * Union companion object.
 */
object Union {
  val MAX_INPUTS = 10
}