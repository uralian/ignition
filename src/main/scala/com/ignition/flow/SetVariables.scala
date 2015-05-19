package com.ignition.flow

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.ignition.types.TypeUtils
import com.ignition.SparkRuntime

/**
 * Sets or drops the ignition runtime variables.
 *
 * @author Vlad Orzhekhovskiy
 */
case class SetVariables(vars: Map[String, Any]) extends Transformer {
  
  override val allInputsRequired: Boolean = false

  protected def compute(arg: DataFrame, limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
    vars foreach {
      case (name, null) => runtime.vars.drop(name)
      case (name, value) => runtime.vars(name) = value
    }
    optLimit(arg, limit)
  }

  protected def computeSchema(inSchema: StructType)(implicit runtime: SparkRuntime): StructType = inSchema

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}

/**
 * SetVariables companion object.
 */
object SetVariables {
  def apply(vars: (String, Any)*): SetVariables = apply(vars.toMap)
}