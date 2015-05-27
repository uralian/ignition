package com.ignition.frame

import scala.util.Random

import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types.{ Decimal, StructType }

import com.ignition.SparkRuntime
import com.ignition.types.RichBoolean

/**
 * Reduce operations.
 */
object ReduceOp extends Enumeration {
  abstract class ReduceOp extends super.Val {
    def reduce(a: Any, b: Any): Any
  }
  implicit def valueToOp(v: Value) = v.asInstanceOf[ReduceOp]

  val ANY = new ReduceOp { def reduce(a: Any, b: Any): Any = Random.nextBoolean ? (a, b) }

  val SUM = new ReduceOp {
    def reduce(a: Any, b: Any): Any = (a, b) match {
      case (x: Int, y: Int) => (x + y)
      case (x: Long, y: Long) => (x + y)
      case (x: Short, y: Short) => (x + y)
      case (x: Byte, y: Byte) => (x + y)
      case (x: Float, y: Float) => (x + y)
      case (x: Double, y: Double) => (x + y)
      case (x: Decimal, y: Decimal) => (x + y)
    }
  }

  val MIN = new ReduceOp {
    def reduce(a: Any, b: Any): Any = (a, b) match {
      case (x: Int, y: Int) => (x < y) ? (x, y)
      case (x: Long, y: Long) => (x < y) ? (x, y)
      case (x: Short, y: Short) => (x < y) ? (x, y)
      case (x: Byte, y: Byte) => (x < y) ? (x, y)
      case (x: Float, y: Float) => (x < y) ? (x, y)
      case (x: Double, y: Double) => (x < y) ? (x, y)
      case (x: Decimal, y: Decimal) => (x < y) ? (x, y)
      case (x: java.sql.Date, y: java.sql.Date) => (x before y) ? (x, y)
      case (x: java.sql.Timestamp, y: java.sql.Timestamp) => (x before y) ? (x, y)
    }
  }

  val MAX = new ReduceOp {
    def reduce(a: Any, b: Any): Any = (a, b) match {
      case (x: Int, y: Int) => (x > y) ? (x, y)
      case (x: Long, y: Long) => (x > y) ? (x, y)
      case (x: Short, y: Short) => (x > y) ? (x, y)
      case (x: Byte, y: Byte) => (x > y) ? (x, y)
      case (x: Float, y: Float) => (x > y) ? (x, y)
      case (x: Double, y: Double) => (x > y) ? (x, y)
      case (x: Decimal, y: Decimal) => (x > y) ? (x, y)
      case (x: java.sql.Date, y: java.sql.Date) => (x after y) ? (x, y)
      case (x: java.sql.Timestamp, y: java.sql.Timestamp) => (x after y) ? (x, y)
    }
  }

  val AND = new ReduceOp {
    def reduce(a: Any, b: Any): Any = (a, b) match {
      case (x: Boolean, y: Boolean) => x && y
    }
  }

  val OR = new ReduceOp {
    def reduce(a: Any, b: Any): Any = (a, b) match {
      case (x: Boolean, y: Boolean) => x || y
    }
  }

  val XOR = new ReduceOp {
    def reduce(a: Any, b: Any): Any = (a, b) match {
      case (x: Boolean, y: Boolean) => (x && !y) || (y && !x)
    }
  }

  val CONCAT = new ReduceOp {
    def reduce(a: Any, b: Any): Any = (a, b) match {
      case (x: String, y: String) => x + y
    }
  }
}
import ReduceOp._

/**
 * Performs reduceByKey() function by grouping the rows by the selected key first, and then
 * applying a list of reduce functions to the specified data columns.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Reduce(reducers: Iterable[(String, ReduceOp)], groupFields: Iterable[String] = Nil)
  extends FrameTransformer with PairFunctions {

  def groupBy(fields: String*) = copy(groupFields = fields)

  protected def compute(arg: DataFrame, limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
    val groupFields = this.groupFields
    val dataFields = reducers map (_._1) toSeq
    val ops = reducers map (_._2) toSeq

    val df = optLimit(arg, limit)

    val rdd = toPair(df, dataFields, groupFields)
    rdd.persist

    val reduced = rdd reduceByKey { (row1, row2) =>
      val values = ops zip (row1.toSeq zip row2.toSeq) map {
        case (op, (a, b)) => op.reduce(a, b)
      }
      Row.fromSeq(values)
    }

    val targetRDD = reduced map {
      case (key, value) => Row.fromSeq(key.toSeq ++ value.toSeq)
    }

    val targetFields = (groupFields map df.schema.apply toSeq) ++ (reducers map {
      case (name, op) => df.schema(name).copy(name = name + "_" + op.toString)
    })
    val targetSchema = StructType(targetFields)

    ctx.createDataFrame(targetRDD, targetSchema)
  }

  protected def computeSchema(inSchema: StructType)(implicit runtime: SparkRuntime): StructType = {
    compute(input(Some(1)), Some(1)) schema
  }

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}

/**
 * Reduce companion object.
 */
object Reduce {
  def apply(reducers: (String, ReduceOp)*): Reduce = apply(reducers.toList)
}