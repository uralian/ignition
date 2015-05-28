package com.ignition.stream

import com.ignition.SparkRuntime
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Queue
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

/**
 * Creates a stream from a static data set, passing one RDD at a time.
 *
 * @author Vlad Orzhekhovskiy
 */
case class QueueInput(schema: StructType, data: List[Seq[Row]] = Nil) extends StreamProducer {

  val dataWithSchema = data map { rows =>
    rows map { row =>
      new GenericRowWithSchema(row.toSeq.toArray, schema).asInstanceOf[Row]
    }
  }

  def addBatch(rdd: Seq[Row]) = copy(data = this.data :+ rdd)

  def addRows(tuples: Any*) = {
    val rs = tuples map {
      case p: Product => Row.fromTuple(p)
      case v => Row(v)
    }
    copy(data = this.data :+ rs)
  }

  protected def compute(limit: Option[Int])(implicit runtime: SparkRuntime): DataStream = {
    val rdds = dataWithSchema map (sc.parallelize(_))

    val queue = Queue(rdds: _*)
    ssc.queueStream(queue, true)
  }

  protected def computeSchema(implicit runtime: SparkRuntime): StructType = schema

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}