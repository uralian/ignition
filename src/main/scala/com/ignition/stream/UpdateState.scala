package com.ignition.stream

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

import com.ignition.SparkRuntime

/**
 * Applies Spark UpdateState function to produce a stream of states.
 *
 * @author Vlad Orzhekhovskiy
 */
abstract class UpdateState(func: (Seq[Row], Option[Row]) => Option[Row],
  dataFields: Iterable[String] = Nil, groupFields: Iterable[String] = Nil)
  extends StreamTransformer with PairFunctions {

  protected def compute(arg: DataStream, limit: Option[Int])(implicit runtime: SparkRuntime): DataStream = {

    val stream = toPair(arg, dataFields, groupFields)
    val states = stream.updateStateByKey(func)

    states map {
      case (key, data) =>
        val schema = StructType(key.schema ++ data.schema)
        val values = key.toSeq ++ data.toSeq
        new GenericRowWithSchema(values.toArray, schema).asInstanceOf[Row]
    }
  }

  protected def computeSchema(inSchema: StructType)(implicit runtime: SparkRuntime): StructType =
    computedSchema(0)
}