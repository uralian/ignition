package com.ignition.flow

import com.ignition.types.TypeUtils._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.ignition.SparkRuntime

/**
 * A boolean condition over a row.
 */
sealed trait RowCondition extends Serializable {
  import RowConditions._

  def apply(row: Row)(implicit runtime: SparkRuntime): Boolean

  def and(that: RowCondition) = AndCondition(this, that)

  def or(that: RowCondition) = OrCondition(this, that)

  def unary_! = NotCondition(this)
}

/**
 * Row condition presets.
 */
object RowConditions {

  case class AndCondition(c1: RowCondition, c2: RowCondition) extends RowCondition {
    def apply(row: Row)(implicit runtime: SparkRuntime) = c1(row) && c2(row)
    override def toString = c1 + " AND " + c2
  }

  case class OrCondition(c1: RowCondition, c2: RowCondition) extends RowCondition {
    def apply(row: Row)(implicit runtime: SparkRuntime) = c1(row) || c2(row)
    override def toString = c1 + " OR " + c2
  }

  case class NotCondition(c: RowCondition) extends RowCondition {
    def apply(row: Row)(implicit runtime: SparkRuntime) = !c(row)
    override def toString = "!" + c
  }
}

/**
 * Variable literal.
 */
case class VarLiteral(name: String) {
  def eval(implicit runtime: SparkRuntime) = runtime.vars(name)
  def evalAs[T](implicit runtime: SparkRuntime) = eval.asInstanceOf[T]
}

/**
 * Environment literal.
 */
case class EnvLiteral(name: String) {
  def eval = System.getProperty(name)
}

/**
 * Field literal.
 */
case class FieldLiteral(name: String) {
  import RowConditions._

  private var index: Option[Int] = None

  case class EqCondition(x: Any) extends RowCondition {
    def apply(row: Row)(implicit runtime: SparkRuntime) = x match {
      case f: FieldLiteral => eval(row) == f.eval(row)
      case v: VarLiteral => eval(row) == v.eval
      case e: EnvLiteral => eval(row) == e.eval
      case n: Number => evalAs[Number](row).doubleValue == n.doubleValue
      case s: String => evalAs[String](row) == s
      case d: java.sql.Date => evalAs[java.sql.Date](row) == x
      case t: java.sql.Timestamp => evalAs[java.sql.Timestamp](row) == x
    }
    override def toString = name + " == " + x
  }

  case class LtCondition(x: Any) extends RowCondition {
    def apply(row: Row)(implicit runtime: SparkRuntime) = x match {
      case f: FieldLiteral => evalAs[Number](row).doubleValue < f.evalAs[Number](row).doubleValue
      case v: VarLiteral => evalAs[Number](row).doubleValue < v.evalAs[Number].doubleValue
      case e: EnvLiteral => evalAs[Number](row).doubleValue < e.eval.toDouble
      case n: Number => evalAs[Number](row).doubleValue < n.doubleValue
      case d: java.sql.Date => evalAs[java.sql.Date](row) before d
      case t: java.sql.Timestamp => evalAs[java.sql.Timestamp](row) before t
    }
    override def toString = name + " < " + x
  }

  case class GtCondition(x: Any) extends RowCondition {
    def apply(row: Row)(implicit runtime: SparkRuntime) = x match {
      case f: FieldLiteral => evalAs[Number](row).doubleValue > f.evalAs[Number](row).doubleValue
      case v: VarLiteral => evalAs[Number](row).doubleValue > v.evalAs[Number].doubleValue
      case e: EnvLiteral => evalAs[Number](row).doubleValue > e.eval.toDouble
      case n: Number => evalAs[Number](row).doubleValue > n.doubleValue
      case d: java.sql.Date => evalAs[java.sql.Date](row) after d
      case t: java.sql.Timestamp => evalAs[java.sql.Timestamp](row) after t
    }
    override def toString = name + " > " + x
  }

  case class InCondition(x: Any*) extends RowCondition {
    def apply(row: Row)(implicit runtime: SparkRuntime) = x contains (eval(row))
    override def toString = name + " in " + x
  }

  case class MatchesCondition(x: Any) extends RowCondition {
    def apply(row: Row)(implicit runtime: SparkRuntime) = x match {
      case f: FieldLiteral => evalAs[String](row) matches f.evalAs[String](row)
      case v: VarLiteral => evalAs[String](row) matches v.eval.toString
      case e: EnvLiteral => evalAs[String](row) matches e.eval.toString
      case s: String => evalAs[String](row) matches s
    }
    override def toString = name + " ~ " + x
  }

  def is(x: Any) = EqCondition(x)
  def == = is _

  def <(x: Any) = LtCondition(x)
  def >(x: Any) = GtCondition(x)
  def <=(x: Any) = LtCondition(x) or EqCondition(x)
  def >=(x: Any) = GtCondition(x) or EqCondition(x)
  def <>(x: Any) = !EqCondition(x)

  def in(x: Any*) = InCondition(x:_*)

  def matches(x: Any) = MatchesCondition(x)
  def ~ = matches _

  def eval(row: Row) = (fieldIndex _ andThen row.get)(row)

  def evalAs[T](row: Row): T = (fieldIndex _ andThen row.getAs[T])(row)

  private def fieldIndex(row: Row) = index getOrElse {
    val idx = row.schema.fieldNames.zipWithIndex.toMap apply (name)
    index = Some(idx)
    idx
  }
}

/**
 * Filters the data frame based on a combination of boolean conditions against fields.
 *
 * @author Vlad Orzhekhovskiy
 */
case class Filter(condition: RowCondition) extends FlowSplitter(2) {
  def and(c: RowCondition) = copy(condition = this.condition.and(c))
  def or(c: RowCondition) = copy(condition = this.condition.or(c))

  protected def compute(arg: DataFrame, index: Int, limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
    val df = optLimit(arg, limit)
    val func = if (index == 0) condition else !condition
    val rdd = df.rdd filter (func(_))
    ctx.createDataFrame(rdd, arg.schema)
  }

  protected def computeSchema(inSchema: StructType, index: Int)(implicit runtime: SparkRuntime): StructType = inSchema

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}