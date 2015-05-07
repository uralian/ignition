package com.ignition.flow

import com.ignition.script.RowExpression
import com.ignition.types.TypeUtils._

import org.apache.spark.sql._
import org.apache.spark.sql.types._

/**
 * Calculates new fields based on string expressions in various dialects.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Formula(fields: Iterable[(String, RowExpression[_ <: DataType])]) extends Transformer {
  
  def addField(name: String, expr: RowExpression[_ <: DataType]) = copy(fields = fields.toSeq :+ (name -> expr))

  protected def compute(arg: DataFrame, limit: Option[Int])(implicit ctx: SQLContext): DataFrame = {
    val df = limit map arg.limit getOrElse arg
    val executors = fields map {
      case (_, expr) => expr.evaluate(df.schema) _
    }

    val rdd = df map { row =>
      val computed = executors map (_(row))
      Row.fromSeq(row.toSeq ++ computed)
    }
    ctx.createDataFrame(rdd, outSchema)
  }

  protected def computeSchema(inSchema: StructType)(implicit ctx: SQLContext): StructType = {
    val df = inputs(Some(1))(ctx)(0)
    val newFields = fields map {
      case (name, expr) =>
        val targetType = expr.targetType getOrElse expr.computeTargetType(inSchema)(df.first)
        StructField(name, targetType, true)
    }
    StructType(inSchema ++ newFields)
  }

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}

/**
 * Formula companion object.
 */
object Formula {
  def apply(fields: (String, RowExpression[_ <: DataType])*): Formula = apply(fields)
}