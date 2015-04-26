package com.ignition.flow

import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.sql.types.StructType

/**
 * Flow input data.
 */
case class FlowInput(schema: Array[StructType]) extends Module(schema.size, schema.size) {

  override val allInputsRequired: Boolean = false

  protected def compute(args: Array[DataFrame],
    index: Int, limit: Option[Int])(implicit ctx: SQLContext): DataFrame = args(index)

  protected def computeSchema(inSchemas: Array[StructType], index: Int)(implicit ctx: SQLContext): StructType =
    schema(index)

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}

/**
 * Flow output data.
 */
case class FlowOutput(override val outputCount: Int) extends Module(outputCount, outputCount) {

  protected def compute(args: Array[DataFrame],
    index: Int, limit: Option[Int])(implicit ctx: SQLContext): DataFrame = args(index)

  protected def computeSchema(inSchemas: Array[StructType], index: Int)(implicit ctx: SQLContext): StructType =
    inSchemas(index)

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}

/**
 * Sub-flow.
 */
case class SubFlow(input: FlowInput, output: FlowOutput) extends Module(input.inputCount, output.outputCount) {

  override def connectFrom(inIndex: Int, step: Step, outIndex: Int): this.type = {
    input.connectFrom(inIndex, step, outIndex)
    this
  }

  override def output(index: Int, limit: Option[Int] = None)(implicit ctx: SQLContext): DataFrame =
    output.output(index, limit)

  override def outSchema(index: Int)(implicit ctx: SQLContext): StructType =
    wrap { output.outSchema(index) }

  /* not used */
  protected def compute(args: Array[DataFrame], index: Int, limit: Option[Int])(implicit ctx: SQLContext): DataFrame = ???

  /* not used */
  protected def computeSchema(inSchemas: Array[StructType], index: Int)(implicit ctx: SQLContext): StructType = ???

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}