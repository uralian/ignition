package com.ignition.flow

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.ignition.types.TypeUtils

/**
 * Action performed by SelectValues step.
 */
sealed trait SelectAction extends Serializable {
  def apply(df: DataFrame): DataFrame
  def apply(schema: StructType): StructType
}

/**
 * Supported actions.
 */
object SelectAction {

  /**
   * Retains only the specified columns.
   */
  case class Retain(names: Iterable[String]) extends SelectAction {
    def apply(df: DataFrame): DataFrame = df.select(names map df.col toSeq: _*)
    def apply(schema: StructType): StructType = StructType(names map schema.apply toSeq)
  }
  object Retain {
    def apply(names: String*): Retain = apply(names.toSeq)
  }

  /**
   * Rename columns.
   */
  case class Rename(dictionary: Map[String, String]) extends SelectAction {
    def apply(df: DataFrame): DataFrame = {
      val columns = df.columns map { name =>
        val oldCol = df.col(name)
        dictionary get name map oldCol.as getOrElse oldCol
      }
      df.select(columns: _*)
    } ensuring (df => checkDuplicates(df.schema), "Duplicate field names")

    def apply(schema: StructType): StructType = {
      val fields = schema map { field =>
        dictionary get (field.name) map (newName => field.copy(name = newName)) getOrElse field
      }
      StructType(fields)
    } ensuring (checkDuplicates(_), "Duplicate field names")

    private def checkDuplicates(schema: StructType) = schema.fieldNames.toSet.size == schema.fields.size
  }
  object Rename {
    def apply(pairs: (String, String)*): Rename = apply(pairs.toMap)
  }

  /**
   * Removes columns.
   */
  case class Remove(names: Iterable[String]) extends SelectAction {
    def apply(df: DataFrame): DataFrame = {
      val fields = df.columns diff names.toSeq
      df.select(fields map df.col toSeq: _*)
    }
    def apply(schema: StructType): StructType = {
      val fields = schema filterNot (f => names.toSeq.contains(f.name))
      StructType(fields)
    }
  }
  object Remove {
    def apply(names: String*): Remove = apply(names.toSeq)
  }

  /**
   * Changes column types.
   */
  case class Retype(dictionary: Map[String, DataType]) extends SelectAction {
    def apply(df: DataFrame): DataFrame = {
      val columns = df.columns map { name =>
        val oldCol = df.col(name)
        dictionary get name map (t => oldCol.cast(t).as(name)) getOrElse oldCol
      }
      df.select(columns: _*)
    }
    def apply(schema: StructType): StructType = {
      val fields = schema map { field =>
        dictionary get field.name map (dt => field.copy(dataType = dt)) getOrElse field
      }
      StructType(fields)
    }
  }
  object Retype {
    def apply(pairs: (String, String)*): Retype = apply(pairs.map {
      case (name, typeName) => (name, TypeUtils.typeForName(typeName))
    }.toMap)
  }
}

/**
 * Modifies, deletes, retains columns in the data rows.
 *
 * @author Vlad Orzhekhovskiy
 */
case class SelectValues(actions: Iterable[SelectAction]) extends Transformer {
  import SelectAction._

  def retain(names: String*) = copy(actions = actions.toSeq :+ Retain(names.toSeq))
  def rename(pairs: (String, String)*) = copy(actions = actions.toSeq :+ Rename(pairs.toMap))
  def remove(names: String*) = copy(actions = actions.toSeq :+ Remove(names.toSeq))
  def retype(pairs: (String, String)*) = copy(actions = actions.toSeq :+ Retype(pairs: _*))

  protected def compute(arg: DataFrame, limit: Option[Int])(implicit ctx: SQLContext): DataFrame = {
    val actions = this.actions

    val df = limit map arg.limit getOrElse arg
    actions.foldLeft(df)((frame, action) => action(frame))
  }

  protected def computeSchema(inSchema: StructType)(implicit ctx: SQLContext): StructType =
    actions.foldLeft(inSchema)((schema, action) => action(schema))

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}

/**
 * Select Values companion object.
 */
object SelectValues {
  def apply(actions: SelectAction*): SelectValues = apply(actions.toSeq)
}