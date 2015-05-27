package com.ignition.frame.mllib

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types.StructType

import com.ignition.SparkRuntime
import com.ignition.frame.FrameTransformer
import com.ignition.types.{ RichStructType, double, fieldToRichStruct, long }

/**
 * Calculates column-based statistics using MLLib library.
 *
 * @author Vlad Orzhekhovskiy
 */
case class ColumnStats(dataFields: Iterable[String] = Nil, groupFields: Iterable[String] = Nil)
  extends FrameTransformer with MLFunctions {

  def columns(fields: String*) = copy(dataFields = fields)
  def groupBy(fields: String*) = copy(groupFields = fields)

  protected def compute(arg: DataFrame, limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
    val df = optLimit(arg, limit)

    val rdd = toVectors(df, dataFields, groupFields)
    rdd.persist

    val keys = rdd.keys.distinct.collect
    val rows = keys map { key =>
      val slice = rdd filter (_._1 == key) values
      val st = Statistics.colStats(slice)

      val data = (0 until dataFields.size) flatMap { idx =>
        Seq(st.max(idx), st.min(idx), st.mean(idx), st.numNonzeros(idx),
          st.variance(idx), st.normL1(idx), st.normL2(idx))
      }
      Row.fromSeq((key.toSeq :+ st.count) ++ data)
    }

    val targetRDD = ctx.sparkContext.parallelize(rows)
    val targetFields = ((groupFields map df.schema.apply toSeq) :+ long("count")) ++
      dataFields.zipWithIndex.flatMap {
        case (name, idx) => double(s"${name}_max") ~ double(s"${name}_min") ~
          double(s"${name}_mean") ~ double(s"${name}_non0") ~ double(s"${name}_variance") ~
          double(s"${name}_normL1") ~ double(s"${name}_normL2")
      }
    val schema = StructType(targetFields)

    ctx.createDataFrame(targetRDD, schema)
  }

  protected def computeSchema(inSchema: StructType)(implicit runtime: SparkRuntime): StructType =
    compute(input(Some(1)), Some(1)) schema

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}