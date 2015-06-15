package com.ignition.frame

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import com.ignition.SparkRuntime

/**
 * Sets or drops the ignition runtime variables.
 *
 * @author Vlad Orzhekhovskiy
 */
case class SetVariables(vars: Map[String, Any]) extends FrameTransformer {

  override val allInputsRequired: Boolean = false

  protected def compute(arg: DataFrame, limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
    vars foreach {
      case (name, null) => runtime.vars.drop(name)
      case (name, value) => runtime.vars(name) = value
    }
    optLimit(arg, limit)
  }

  protected def computeSchema(inSchema: StructType)(implicit runtime: SparkRuntime): StructType = inSchema
}

/**
 * SetVariables companion object.
 */
object SetVariables {
  def apply(vars: (String, Any)*): SetVariables = apply(vars.toMap)
}