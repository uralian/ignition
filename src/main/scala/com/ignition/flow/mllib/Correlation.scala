package com.ignition.flow.mllib

import org.apache.spark.sql.{ DataFrame, SQLContext, Row }
import org.apache.spark.sql.types.StructType
import org.apache.spark.mllib.stat.Statistics

import com.ignition.types.double

/**
 * Correlation methods.
 */
object CorrelationMethod extends Enumeration {
  type CorrelationMethod = Value

  val PEARSON = Value("pearson")
  val SPEARMAN = Value("spearman")
}
import CorrelationMethod._

/**
 * Computes the correlation between data series.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Correlation(fields: Iterable[String] = Nil, groupFields: Iterable[String] = Nil,
  method: CorrelationMethod = PEARSON) extends AbstractMLStep {

  def columns(fields: String*) = copy(fields = fields)
  def groupBy(fields: String*) = copy(groupFields = fields)
  def method(method: CorrelationMethod) = copy(method = method)

  protected def compute(arg: DataFrame, limit: Option[Int])(implicit ctx: SQLContext): DataFrame = {
    val df = optLimit(arg, limit)

    val rdd = partitionByKey(df, fields, groupFields)
    rdd.persist

    val keys = rdd.keys.distinct.collect
    val rows = keys map { key =>
      val slice = rdd filter (_._1 == key) values
      val matrix = Statistics.corr(slice)

      val data = for {
        rowIdx <- 0 until fields.size
        colIdx <- rowIdx + 1 until fields.size
      } yield matrix(rowIdx, colIdx)
      Row.fromSeq(key.toSeq ++ data)
    }

    val targetRDD = ctx.sparkContext.parallelize(rows)
    val fieldSeq = fields.toSeq
    val targetFields = (groupFields map df.schema.apply toSeq) ++ (for {
      rowIdx <- 0 until fields.size
      colIdx <- rowIdx + 1 until fields.size
    } yield double(s"corr_${fieldSeq(rowIdx)}_${fieldSeq(colIdx)}"))
    val schema = StructType(targetFields)

    ctx.createDataFrame(targetRDD, schema)
  }

  protected def computeSchema(inSchema: StructType)(implicit ctx: SQLContext): StructType =
    compute(input(Some(100)), Some(100)) schema

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}