package com.ignition.frame

import org.apache.spark.sql.{Column, ColumnName, Row}

import com.ignition.SparkRuntime

/**
 * A boolean condition over a row.
 */
sealed trait RowCondition extends Serializable {
  import RowConditions._

  def apply(row: Row)(implicit runtime: SparkRuntime): Boolean

  def column(implicit runtime: SparkRuntime): Column

  def and(that: RowCondition) = AndCondition(this, that)

  def or(that: RowCondition) = OrCondition(this, that)

  def unary_! = NotCondition(this)
}

/**
 * Row condition presets.
 */
object RowConditions {

  /*
   * Logical operators
   */
  case class AndCondition(c1: RowCondition, c2: RowCondition) extends RowCondition {
    def apply(row: Row)(implicit runtime: SparkRuntime) = c1(row) && c2(row)
    def column(implicit runtime: SparkRuntime) = c1.column && c2.column
    override def toString = c1 + " AND " + c2
  }

  case class OrCondition(c1: RowCondition, c2: RowCondition) extends RowCondition {
    def apply(row: Row)(implicit runtime: SparkRuntime) = c1(row) || c2(row)
    def column(implicit runtime: SparkRuntime) = c1.column || c2.column
    override def toString = c1 + " OR " + c2
  }

  case class NotCondition(c: RowCondition) extends RowCondition {
    def apply(row: Row)(implicit runtime: SparkRuntime) = !c(row)
    def column(implicit runtime: SparkRuntime) = !c.column
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
case class FieldLiteral(name: String) { self =>
  import RowConditions._

  private var index: Option[Int] = None

  def column(implicit runtime: SparkRuntime) = new ColumnName(name)

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
    def column(implicit runtime: SparkRuntime) = x match {
      case f: FieldLiteral => self.column === f.column
      case v: VarLiteral => self.column === v.eval
      case e: EnvLiteral => self.column === e.eval
      case t: Any => self.column === t
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
    def column(implicit runtime: SparkRuntime) = x match {
      case f: FieldLiteral => self.column < f.column
      case v: VarLiteral => self.column < v.eval
      case e: EnvLiteral => self.column < e.eval
      case t: Any => self.column < t
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
    def column(implicit runtime: SparkRuntime) = x match {
      case f: FieldLiteral => self.column > f.column
      case v: VarLiteral => self.column > v.eval
      case e: EnvLiteral => self.column > e.eval
      case t: Any => self.column > t
    }
    override def toString = name + " > " + x
  }

  case class InCondition(x: Any*) extends RowCondition {
    def apply(row: Row)(implicit runtime: SparkRuntime) = x contains (eval(row))
    def column(implicit runtime: SparkRuntime) = {
      val right = x map {
        case f: FieldLiteral => f.column
        case v: VarLiteral => new Column(v.evalAs[String])
        case e: EnvLiteral => new Column(e.eval)
        case t: Any => new Column(t.toString)
      }
      self.column.in(right: _*)
    }
    override def toString = name + " in " + x
  }

  case class MatchesCondition(x: Any) extends RowCondition {
    def apply(row: Row)(implicit runtime: SparkRuntime) = x match {
      case f: FieldLiteral => evalAs[String](row) matches f.evalAs[String](row)
      case v: VarLiteral => evalAs[String](row) matches v.eval.toString
      case e: EnvLiteral => evalAs[String](row) matches e.eval.toString
      case s: String => evalAs[String](row) matches s
    }
    def column(implicit runtime: SparkRuntime) = x match {
      case v: VarLiteral => self.column rlike v.evalAs[String]
      case e: EnvLiteral => self.column rlike e.eval
      case t: Any => self.column rlike t.toString
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

  def in(x: Any*) = InCondition(x: _*)

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