package com.ignition.flow

import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.sql.types.StructType

/**
 * Flow input data.
 */
case class FlowInput(inputMeta: Array[StructType]) extends AbstractStep(inputMeta.size, inputMeta.size) {

  override protected val allInputsRequired: Boolean = false

  protected def compute(args: Array[DataFrame],
    index: Int)(implicit ctx: SQLContext): DataFrame = args(index)

  protected def computeSchema(inSchemas: Array[Option[StructType]],
    index: Int)(implicit ctx: SQLContext): Option[StructType] = Some(inputMeta(index))

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}

/**
 * Flow output data.
 */
case class FlowOutput(override val outputCount: Int) extends AbstractStep(outputCount, outputCount) {

  protected def compute(args: Array[DataFrame],
    index: Int)(implicit ctx: SQLContext): DataFrame = args(index)

  protected def computeSchema(inSchemas: Array[Option[StructType]],
    index: Int)(implicit ctx: SQLContext): Option[StructType] = inSchemas(index)

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}

/**
 * Sub-flow.
 */
case class SubFlow(input: FlowInput, output: FlowOutput) extends AbstractStep(input.inputCount, output.outputCount) {

  override def connectFrom(inIndex: Int, step: Step, outIndex: Int): AbstractStep =
    input.connectFrom(inIndex, step, outIndex)

  override def output(index: Int)(implicit ctx: SQLContext): DataFrame = output.output(index)

  override def outputSchema(index: Int)(implicit ctx: SQLContext): Option[StructType] = output.outputSchema(index)

  protected def compute(args: Array[DataFrame], index: Int)(implicit ctx: SQLContext): DataFrame = ???

  protected def computeSchema(inSchemas: Array[Option[StructType]], index: Int)(implicit ctx: SQLContext): Option[StructType] = ???

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}