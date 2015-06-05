package com.ignition.frame

import org.apache.spark.sql.{ Column, DataFrame, SQLContext }
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

import com.ignition.SparkRuntime

/**
 * Basic aggregate functions.
 */
object BasicAggregator extends Enumeration {

  class BasicAggregator(func: Column => Column, nameFunc: String => String) extends super.Val {
    def createColumn(df: DataFrame, field: String) = (df.col _ andThen func)(field).as(nameFunc(field))
  }
  implicit def valueToAggregator(v: Value) = v.asInstanceOf[BasicAggregator]

  val AVG = new BasicAggregator(avg, _ + "_avg")
  val MIN = new BasicAggregator(min, _ + "_min")
  val MAX = new BasicAggregator(max, _ + "_max")
  val SUM = new BasicAggregator(sum, _ + "_sum")
  val COUNT = new BasicAggregator(count, _ + "_cnt")
  val FIRST = new BasicAggregator(first, _ + "_first")
  val LAST = new BasicAggregator(last, _ + "_last")
  val SUM_DISTINCT = new BasicAggregator(sumDistinct, _ + "_sum_dist")
  val COUNT_DISTINCT = new BasicAggregator(countDistinct(_), _ + "_cnt_dist")
}
import BasicAggregator._

/**
 * Calculates basic statistics about the specified fields.
 *
 * @author Vlad Orzhekhovskiy
 */
case class BasicStats(fields: Map[String, Set[BasicAggregator]] = Map.empty,
  groupFields: Iterable[String] = Nil) extends FrameTransformer {

  def groupBy(fields: String*) = copy(groupFields = fields)
  def aggr(name: String, functions: BasicAggregator*) =
    copy(fields = fields + (name -> functions.toSet))

  protected def compute(arg: DataFrame, limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
    val df = optLimit(arg, limit)

    val groupColumns = groupFields map df.col toSeq
    val aggrColumns = fields.toList flatMap {
      case (name, funcs) => funcs map (f => f.createColumn(df, name))
    }
    val columns = groupColumns ++ aggrColumns
    df.groupBy(groupColumns: _*).agg(columns.head, columns.tail: _*)
  }

  protected def computeSchema(inSchema: StructType)(implicit runtime: SparkRuntime): StructType =
    computedSchema(0)

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}