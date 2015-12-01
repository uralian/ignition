package com.ignition.stream

import scala.reflect.ClassTag

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

import com.ignition.{ Merger, Module, Producer, Splitter, Step, Transformer }

/**
 * Workflow step that emits DataStream as the output.
 */
trait StreamStep extends Step[DataStream, SparkStreamingRuntime] {

  /**
   * Returns the implicit SQLContext.
   */
  protected def ctx(implicit runtime: SparkStreamingRuntime) = runtime.ctx

  /**
   * Returns the implicit SparkContext.
   */
  protected def sc(implicit runtime: SparkStreamingRuntime) = runtime.sc

  /**
   * Returns the implicit StreamingContext.
   */
  protected def ssc(implicit runtime: SparkStreamingRuntime) = runtime.ssc
}

/* step templates */

abstract class StreamProducer extends Producer[DataStream, SparkStreamingRuntime] with StreamStep

abstract class StreamTransformer extends Transformer[DataStream, SparkStreamingRuntime] with StreamStep

abstract class StreamSplitter(val outputCount: Int)
  extends Splitter[DataStream, SparkStreamingRuntime] with StreamStep

abstract class StreamMerger(val inputCount: Int)
  extends Merger[DataStream, SparkStreamingRuntime] with StreamStep

abstract class StreamModule(val inputCount: Int, val outputCount: Int)
  extends Module[DataStream, SparkStreamingRuntime] with StreamStep

/* update state */

/**
 * Applies Spark updateStateByKey function to produce a stream of states.
 * The type parameter S defines the state class.
 *
 * 1. First the original DStream[Row] is converted to
 * DStream[(Row, Row)] where the key is a subrow defined by the `keyFields`, and the value is the
 * original row.
 * 2. Then `updateStateByKey` function is used to produce the stream of states DStream[(Row, S)].
 * 3. Finally each state is converted into a sequence of rows with `mapFunc`, which are combined
 * with the key, and the resulting stream is DStream[Row].
 *
 * @param stateFunc function passed to Spark updateStateByKey function.
 * @param mapFunc function that converts a state into a sequence of rows.
 * @param keyFields fields constituting the key.
 *
 * @author Vlad Orzhekhovskiy
 */
abstract class StateUpdate[S: ClassTag](keyFields: Iterable[String]) extends StreamTransformer with PairFunctions {

  /**
   * The function to pass to `updateStateByKey`.
   */
  def stateFunc(input: Seq[Row], oldState: Option[S]): Option[S]

  /**
   * Converts the state into a sequence of rows.
   */
  def mapFunc(state: S): Iterable[Row]

  protected def compute(arg: DataStream, preview: Boolean)(implicit runtime: SparkStreamingRuntime): DataStream = {

    val stream = toPair(arg, Nil, keyFields)

    val states = stream updateStateByKey stateFunc flatMapValues mapFunc

    states map {
      case (key, data) =>
        val schema = StructType(key.schema ++ data.schema)
        val values = key.toSeq ++ data.toSeq
        new GenericRowWithSchema(values.toArray, schema).asInstanceOf[Row]
    }
  }
}
