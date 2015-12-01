package com.ignition.frame

import scala.util.Random
import scala.xml.{ Elem, Node }

import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types.{ Decimal, StructType }

import com.ignition.types.RichBoolean

import org.json4s._
import org.json4s.JsonDSL._

import com.ignition.util.XmlUtils.RichNodeSeq
import com.ignition.util.JsonUtils.RichJValue

/**
 * Reduce operations.
 */
object ReduceOp extends Enumeration {
  abstract class ReduceOp extends super.Val {
    def reduce(a: Any, b: Any): Any
    def apply(field: String) = field -> this
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

  import Reduce._

  def add(tuple: (String, ReduceOp)) = copy(reducers = reducers.toSeq :+ tuple)
  def %(tuple: (String, ReduceOp)) = add(tuple)

  def add(field: String, functions: ReduceOp*) = copy(functions.map(f => (field, f)))
  def %(field: String, functions: ReduceOp*) = add(field, functions: _*)

  def groupBy(fields: String*) = copy(groupFields = fields)

  protected def compute(arg: DataFrame, preview: Boolean)(implicit runtime: SparkRuntime): DataFrame = {
    val groupFields = this.groupFields
    val dataFields = reducers map (_._1) toSeq
    val ops = reducers map (_._2) toSeq

    val df = optLimit(arg, preview)

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

  def toXml: Elem =
    <node>
      <aggregate>
        {
          reducers map { case (name, op) => <field name={ name } type={ op.toString }/> }
        }
      </aggregate>
      {
        if (!groupFields.isEmpty)
          <group-by>
            { groupFields map (f => <field name={ f }/>) }
          </group-by>
      }
    </node>.copy(label = tag)

  def toJson: JValue = {
    val groupBy = if (groupFields.isEmpty) None else Some(groupFields)
    val aggregate = reducers map (df => df._1 -> df._2.toString)
    ("tag" -> tag) ~ ("groupBy" -> groupBy) ~ ("aggregate" -> aggregate)
  }
}

/**
 * Reduce companion object.
 */
object Reduce {
  val tag = "reduce"

  def apply(reducers: (String, ReduceOp)*): Reduce = apply(reducers.toList)

  def fromXml(xml: Node) = {
    val dataFields = (xml \ "aggregate" \ "field") map { node =>
      val name = node \ "@name" asString
      val func = ReduceOp.withName(node \ "@type" asString): ReduceOp
      name -> func
    }
    val groupFields = (xml \ "group-by" \ "field") map (_ \ "@name" asString)
    apply(dataFields, groupFields)
  }

  def fromJson(json: JValue) = {
    val dataFields = for {
      JObject(df) <- json \ "aggregate"
      JField(name, JString(funcName)) <- df
      func = ReduceOp.withName(funcName): ReduceOp
    } yield name -> func
    val groupFields = (json \ "groupBy" asArray) map (_ asString)
    apply(dataFields, groupFields)
  }
}