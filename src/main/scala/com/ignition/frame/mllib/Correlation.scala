package com.ignition.frame.mllib

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types.StructType

import com.ignition.SparkRuntime
import com.ignition.frame.FrameTransformer
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
 * Computes the correlation between data series using MLLib library.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Correlation(dataFields: Iterable[String] = Nil, groupFields: Iterable[String] = Nil,
  method: CorrelationMethod = PEARSON) extends FrameTransformer with MLFunctions {

  def columns(fields: String*) = copy(dataFields = fields)
  def groupBy(fields: String*) = copy(groupFields = fields)
  def method(method: CorrelationMethod) = copy(method = method)

  protected def compute(arg: DataFrame, limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
    val df = optLimit(arg, limit)

    val rdd = toVectors(df, dataFields, groupFields)
    rdd.persist

    val keys = rdd.keys.distinct.collect
    val rows = keys map { key =>
      val slice = rdd filter (_._1 == key) values
      val matrix = Statistics.corr(slice)

      val data = for {
        rowIdx <- 0 until dataFields.size
        colIdx <- rowIdx + 1 until dataFields.size
      } yield matrix(rowIdx, colIdx)
      Row.fromSeq(key.toSeq ++ data)
    }

    val targetRDD = ctx.sparkContext.parallelize(rows)
    val fieldSeq = dataFields.toSeq
    val targetFields = (groupFields map df.schema.apply toSeq) ++ (for {
      rowIdx <- 0 until dataFields.size
      colIdx <- rowIdx + 1 until dataFields.size
    } yield double(s"corr_${fieldSeq(rowIdx)}_${fieldSeq(colIdx)}"))
    val schema = StructType(targetFields)

    ctx.createDataFrame(targetRDD, schema)
  }

  protected def computeSchema(inSchema: StructType)(implicit runtime: SparkRuntime): StructType =
    compute(input(Some(100)), Some(100)) schema

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}