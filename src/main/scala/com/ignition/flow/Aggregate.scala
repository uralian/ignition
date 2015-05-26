package com.ignition.flow

import scala.reflect.{ ClassTag, classTag }

import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types.StructType

import com.ignition.SparkRuntime

/**
 * Aggregates a data row into some arbitrary class U using Spark aggregateByKey method.
 * Once the aggregation is done, the resulting value of type U is converted back to a
 * data row.
 */
trait RowAggregator[U] extends Serializable {
  implicit val ctag: ClassTag[U] = classTag[U]

  /**
   * Provides the "zero value" for aggregation.
   */
  def zero: U

  /**
   * Merges a data row into the aggregated value.
   */
  def seqOp(value: U, row: Row): U

  /**
   * Combines the two aggregated values.
   */
  def combOp(value1: U, value2: U): U

  /**
   * Converts the aggregated value into a Row.
   */
  def toRow(value: U): Row

  /**
   * Returns the schema of the aggregated rows.
   */
  def schema: StructType
}

/**
 * An abstract Aggregate step, which uses the list of grouping fields to partition
 * the data supplied row aggregator to aggregate each partition.
 */
abstract class AbstractAggregate[U: ClassTag](aggregator: RowAggregator[U], groupFields: Iterable[String] = Nil)
  extends FlowTransformer with PairFunctions {

  protected def compute(arg: DataFrame, limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
    val groupFields = this.groupFields

    val df = optLimit(arg, limit)

    val rdd = toPair(df, df.schema.fieldNames, groupFields)
    rdd.persist

    val aggregated = rdd.aggregateByKey(aggregator.zero)(aggregator.seqOp, aggregator.combOp)

    val targetRDD = aggregated map {
      case (key, value) => Row.fromSeq(key.toSeq ++ aggregator.toRow(value).toSeq)
    }

    val targetFields = (groupFields map df.schema.apply toSeq) ++ aggregator.schema
    val targetSchema = StructType(targetFields)

    ctx.createDataFrame(targetRDD, targetSchema)
  }

  protected def computeSchema(inSchema: StructType)(implicit runtime: SparkRuntime): StructType = {
    compute(input(Some(1)), Some(1)) schema
  }
}