package com.ignition.frame

import scala.xml.{ Elem, Node }

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ DataType, StructType }
import org.json4s.{ JString, JValue }
import org.json4s.JsonDSL.{ pair2Assoc, seq2jvalue, string2jvalue }
import org.json4s.jvalue2monadic

import com.ignition.SparkRuntime
import com.ignition.types.TypeUtils
import com.ignition.types.TypeUtils.typeForName
import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.RichNodeSeq

/**
 * Action performed by SelectValues step.
 */
sealed trait SelectAction extends Serializable {
  def apply(df: DataFrame): DataFrame
  def apply(schema: StructType): StructType
  def toXml: Elem
  def toJson: JValue
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
    def toXml: Elem = <retain>{ names map (n => <field name={ n }/>) }</retain>
    def toJson: JValue = ("action" -> "retain") ~ ("fields" -> names)
  }
  object Retain {
    def apply(names: String*): Retain = apply(names.toSeq)
    def fromXml(xml: Node) = Retain(xml \ "field" map (_ \ "@name" asString))
    def fromJson(json: JValue) = Retain((json \ "fields" asArray) map (_ asString))
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

    def toXml: Elem = <rename>{ dictionary map (n => <field oldName={ n._1 } newName={ n._2 }/>) }</rename>

    def toJson: JValue = ("action" -> "rename") ~
      ("fields" -> dictionary.map(t => ("oldName" -> t._1) ~ ("newName" -> t._2)))

    private def checkDuplicates(schema: StructType) = schema.fieldNames.toSet.size == schema.fields.size
  }
  object Rename {
    def apply(pairs: (String, String)*): Rename = apply(pairs.toMap)
    def fromXml(xml: Node) = {
      val dictionary = xml \ "field" map { node =>
        val oldName = node \ "@oldName" asString
        val newName = node \ "@newName" asString

        oldName -> newName
      }
      Rename(dictionary.toMap)
    }
    def fromJson(json: JValue) = {
      val dictionary = (json \ "fields" asArray) map { node =>
        val oldName = node \ "oldName" asString
        val newName = node \ "newName" asString

        oldName -> newName
      }
      Rename(dictionary.toMap)
    }
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
    def toXml: Elem = <remove>{ names map (n => <field name={ n }/>) }</remove>
    def toJson: JValue = ("action" -> "remove") ~ ("fields" -> names)
  }
  object Remove {
    def apply(names: String*): Remove = apply(names.toSeq)
    def fromXml(xml: Node) = Remove(xml \ "field" map (_ \ "@name" asString))
    def fromJson(json: JValue) = Remove((json \ "fields" asArray) map (_ asString))
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
    def toXml: Elem = <retype>{ dictionary map (n => <field name={ n._1 } type={ n._2.typeName }/>) }</retype>
    def toJson: JValue = ("action" -> "retype") ~
      ("fields" -> dictionary.map(t => ("name" -> t._1) ~ ("type" -> t._2.typeName)))
  }
  object Retype {
    def apply(pairs: (String, String)*): Retype = apply(pairs.map {
      case (name, typeName) => (name, TypeUtils.typeForName(typeName))
    }.toMap)
    def fromXml(xml: Node) = {
      val dictionary = xml \ "field" map { node =>
        val name = node \ "@name" asString
        val dataType = typeForName(node \ "@type" asString)

        name -> dataType
      }
      Retype(dictionary.toMap)
    }
    def fromJson(json: JValue) = {
      val dictionary = (json \ "fields" asArray) map { node =>
        val name = node \ "name" asString
        val dataType = typeForName(node \ "type" asString)

        name -> dataType
      }
      Retype(dictionary.toMap)
    }
  }

  def fromXml(xml: Node) = scala.xml.Utility.trim(xml) match {
    case <rename>{ _* }</rename> => Rename.fromXml(xml)
    case <remove>{ _* }</remove> => Remove.fromXml(xml)
    case <retain>{ _* }</retain> => Retain.fromXml(xml)
    case <retype>{ _* }</retype> => Retype.fromXml(xml)
  }

  def fromJson(json: JValue) = json \ "action" match {
    case JString("rename") => Rename.fromJson(json)
    case JString("remove") => Remove.fromJson(json)
    case JString("retain") => Retain.fromJson(json)
    case JString("retype") => Retype.fromJson(json)
    case x => throw new IllegalArgumentException(s"Unknown action: $x")
  }
}

/**
 * Modifies, deletes, retains columns in the data rows.
 *
 * @author Vlad Orzhekhovskiy
 */
case class SelectValues(actions: Iterable[SelectAction]) extends FrameTransformer {
  import SelectValues._
  import SelectAction._

  def retain(names: String*) = copy(actions = actions.toSeq :+ Retain(names.toSeq))
  def rename(pairs: (String, String)*) = copy(actions = actions.toSeq :+ Rename(pairs.toMap))
  def remove(names: String*) = copy(actions = actions.toSeq :+ Remove(names.toSeq))
  def retype(pairs: (String, String)*) = copy(actions = actions.toSeq :+ Retype(pairs: _*))

  protected def compute(arg: DataFrame, preview: Boolean)(implicit runtime: SparkRuntime): DataFrame = {
    val actions = this.actions

    val df = optLimit(arg, preview)
    actions.foldLeft(df)((frame, action) => action(frame))
  }

  override protected def buildSchema(index: Int)(implicit runtime: SparkRuntime): StructType =
    actions.foldLeft(input(true).schema)((schema, action) => action(schema))

  def toXml: Elem = <node>{ actions map (_.toXml) }</node>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag) ~ ("actions" -> actions.map(_.toJson))
}

/**
 * Select Values companion object.
 */
object SelectValues {
  val tag = "select-values"

  def apply(actions: SelectAction*): SelectValues = apply(actions.toSeq)

  def fromXml(xml: Node) = apply(xml \ "_" map SelectAction.fromXml)

  def fromJson(json: JValue) = apply((json \ "actions" asArray) map SelectAction.fromJson)
}