package com.ignition.frame

import org.apache.spark.sql._
import com.ignition.SparkRuntime
import org.apache.spark.sql.types.StructType

/**
 * DataFrame join type.
 */
object JoinType extends Enumeration {
  type JoinType = Value

  val INNER = Value("inner")
  val OUTER = Value("outer")
  val LEFT = Value("left_outer")
  val RIGHT = Value("right_outer")
  val SEMI = Value("semijoin")
}
import JoinType._

/**
 * Performs join of the two data frames.
 * In row conditions, if there is ambiguity in a field's name, use "input0" and "input1"
 * prefixes for the first and second input respectively.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Join(condition: Option[RowCondition], joinType: JoinType) extends FrameMerger(2) {
  
  def joinType(jt: JoinType) = copy(joinType = jt)

  protected def compute(args: Seq[DataFrame], limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {

    val df1 = optLimit(args(0), limit).as('input0)
    val df2 = optLimit(args(1), limit).as('input1)

    condition map (c => df1.join(df2, c.column, joinType.toString)) getOrElse df1.join(df2)
  }

  protected def computeSchema(inSchemas: Seq[StructType])(implicit runtime: SparkRuntime): StructType =
    computedSchema(0)
}

object Join {
  def apply(): Join = apply(None, INNER)
  def apply(condition: RowCondition, joinType: JoinType = INNER): Join = apply(Some(condition), joinType)
}