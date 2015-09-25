package com.ignition.stream

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import com.ignition.SparkRuntime

/**
 * Sets or drops the ignition runtime variables.
 *
 * @author Vlad Orzhekhovskiy
 */
case class SetVariables(vars: Map[String, Any]) extends StreamTransformer {

  override val allInputsRequired: Boolean = false

  protected def compute(arg: DataStream, limit: Option[Int])(implicit runtime: SparkRuntime): DataStream = {
    vars foreach {
      case (name, null) => runtime.vars.drop(name)
      case (name, value) => runtime.vars(name) = value
    }
    arg
  }

  protected def computeSchema(inSchema: StructType)(implicit runtime: SparkRuntime): StructType = inSchema
  
  def toXml: scala.xml.Elem = ???
  def toJson: org.json4s.JValue = ???
}

/**
 * SetVariables companion object.
 */
object SetVariables {
  def apply(vars: (String, Any)*): SetVariables = apply(vars.toMap)
}