package com.ignition.stream

import scala.reflect.ClassTag

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

import com.ignition._

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

/* subflow templates */

trait StreamSubFlow extends StreamStep {
  val tag = StreamSubFlow.tag
}

case class StreamSubProducer(body: ConnectionSource[DataStream, SparkStreamingRuntime])
  extends SubProducer[DataStream, SparkStreamingRuntime](body) with StreamSubFlow

case class StreamSubTransformer(body: (ConnectionTarget[DataStream, SparkStreamingRuntime], ConnectionSource[DataStream, SparkStreamingRuntime]))
  extends SubTransformer[DataStream, SparkStreamingRuntime](body) with StreamSubFlow

case class StreamSubSplitter(body: (ConnectionTarget[DataStream, SparkStreamingRuntime], Seq[ConnectionSource[DataStream, SparkStreamingRuntime]]))
  extends SubSplitter[DataStream, SparkStreamingRuntime](body) with StreamSubFlow

case class StreamSubMerger(body: (Seq[ConnectionTarget[DataStream, SparkStreamingRuntime]], ConnectionSource[DataStream, SparkStreamingRuntime]))
  extends SubMerger[DataStream, SparkStreamingRuntime](body) with StreamSubFlow

case class StreamSubModule(body: (Seq[ConnectionTarget[DataStream, SparkStreamingRuntime]], Seq[ConnectionSource[DataStream, SparkStreamingRuntime]]))
  extends SubModule[DataStream, SparkStreamingRuntime](body) with StreamSubFlow

/**
 * Provides SubFlow common methods.
 */
object StreamSubFlow extends SubFlowFactory[StreamStep, DataStream, SparkStreamingRuntime] {

  val tag = "stream-subflow"

  val xmlFactory = StreamStepFactory

  val jsonFactory = StreamStepFactory

  /**
   * Depending on the number of inputs and outputs, the returned instance can be a
   * StreamSubProducer, StreamSubTransformer, FrameSubSplitter,
   * StreamSubMerger, or StreamSubModule.
   */
  def instantiate(
    inPoints: Seq[ConnectionTarget[DataStream, SparkStreamingRuntime]],
    outPoints: Seq[ConnectionSource[DataStream, SparkStreamingRuntime]]): StreamStep with SubFlow[DataStream, SparkStreamingRuntime] =
    (inPoints.size, outPoints.size) match {
      case (0, 1)          => StreamSubProducer(outPoints(0))
      case (1, 1)          => StreamSubTransformer((inPoints(0), outPoints(0)))
      case (1, o) if o > 1 => StreamSubSplitter((inPoints(0), outPoints))
      case (i, 1) if i > 1 => StreamSubMerger((inPoints, outPoints(0)))
      case _               => StreamSubModule((inPoints, outPoints))
    }
}

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
