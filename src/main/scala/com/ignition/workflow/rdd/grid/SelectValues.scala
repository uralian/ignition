package com.ignition.workflow.rdd.grid

import scala.collection.TraversableOnce.MonadOps
import scala.xml.{ Elem, Node, Utility }

import org.apache.spark.rdd.RDD

import com.ignition.data.{ ColumnInfo, DataRow, DataType, DefaultDataRow, DefaultRowMetaData, RowMetaData }

/**
 * Action performed by SelectValues step.
 */
sealed trait SelectAction extends Serializable {
  def apply(row: DataRow): DataRow
  def apply(meta: RowMetaData): RowMetaData
  def toXml: Elem
}

/**
 * Supported actions.
 */
object SelectAction extends XmlFactory[SelectAction] {

  /**
   * Retains only the specified columns.
   */
  case class Retain(names: TraversableOnce[String]) extends SelectAction {
    def apply(row: DataRow): DataRow = {
      val rawData = names map row.getRaw
      DefaultDataRow(names.toIndexedSeq, rawData.toIndexedSeq)
    }
    def apply(meta: RowMetaData): RowMetaData = {
      val columns = names map (meta.columnIndex _ andThen meta.columns.apply)
      DefaultRowMetaData(columns.toIndexedSeq)
    }
    def toXml: Elem = <retain>{ names map (n => <col name={ n }/>) }</retain>
  }
  object Retain extends XmlFactory[Retain] {
    def apply(names: String*): Retain = apply(names.toSeq)
    def fromXml(xml: Node): Retain = apply((xml \ "col" \\ "@name") map (_.text))
  }

  /**
   * Rename columns.
   */
  case class Rename(dictionary: Map[String, String]) extends SelectAction {
    def apply(row: DataRow): DataRow = {
      val newNames = row.columnNames map { name => dictionary.getOrElse(name, name) }
      DefaultDataRow(newNames, row.rawData)
    }
    def apply(meta: RowMetaData): RowMetaData = {
      val newColumns = meta.columns map { ci =>
        val newName = dictionary.getOrElse(ci.name, ci.name)
        ColumnInfo(newName)(ci.dataType)
      }
      DefaultRowMetaData(newColumns)
    }
    def toXml: Elem = <rename>{ dictionary map { case (ol, nw) => <col old={ ol } new={ nw }/> } }</rename>
  }
  object Rename extends XmlFactory[Rename] {
    def apply(pairs: (String, String)*): Rename = apply(pairs.toMap)
    def fromXml(xml: Node): Rename = {
      val pairs = (xml \ "col") map { n =>
        val ol = (n \ "@old").text
        val nw = (n \ "@new").text
        (ol, nw)
      }
      apply(pairs.toMap)
    }
  }

  /**
   * Removes columns.
   */
  case class Remove(names: TraversableOnce[String]) extends SelectAction {
    def apply(row: DataRow): DataRow = names.foldLeft(row)((row, name) => {
      val index = row.columnNames.indexOf(name)
      if (index < 0) row else {
        val newNames = row.columnNames.patch(index, Nil, 1)
        val newValues = row.rawData.patch(index, Nil, 1)
        DefaultDataRow(newNames, newValues)
      }
    })
    def apply(meta: RowMetaData): RowMetaData = names.foldLeft(meta)((meta, name) => {
      val index = meta.columnIndex(name)
      if (index < 0) meta else DefaultRowMetaData(meta.columns.patch(index, Nil, 1))
    })
    def toXml: Elem = <remove>{ names map (n => <col name={ n }/>) }</remove>
  }
  object Remove extends XmlFactory[Remove] {
    def apply(names: String*): Remove = apply(names.toSeq)
    def fromXml(xml: Node): Remove = apply((xml \ "col" \\ "@name") map (_.text))
  }

  /**
   * Changes column types.
   */
  case class Retype(dictionary: Map[String, DataType[_]]) extends SelectAction {
    def apply(row: DataRow): DataRow = dictionary.foldLeft(row)((row, pair) => {
      val index = row.columnNames.indexOf(pair._1)
      if (index < 0) row else {
        val newValue = pair._2.convert(row.getRaw(index))
        val newValues = row.rawData.patch(index, List(newValue), 1)
        DefaultDataRow(row.columnNames, newValues)
      }
    })
    def apply(meta: RowMetaData): RowMetaData = dictionary.foldLeft(meta)((meta, pair) => {
      val index = meta.columnIndex(pair._1)
      if (index < 0) meta else {
        val newColumn = ColumnInfo(pair._1)(pair._2)
        val newColumns = meta.columns.patch(index, List(newColumn), 1)
        DefaultRowMetaData(newColumns)
      }
    })
    def toXml: Elem = <retype>{ dictionary map { case (name, dt) => <col name={ name } type={ dt.code }/> } }</retype>
  }
  object Retype extends XmlFactory[Retype] {
    def apply(pairs: (String, DataType[_])*): Retype = apply(pairs.toMap)
    def fromXml(xml: Node): Retype = {
      val pairs: Seq[(String, DataType[_])] = (xml \ "col") map { n =>
        val name = (n \ "@name").text
        val dt = DataType.withCode((n \ "@type").text)
        (name, dt)
      }
      apply(pairs.toMap)
    }
  }

  def fromXml(xml: Node): SelectAction = Utility.trim(xml) match {
    case a @ <retain>{ _* }</retain> => Retain.fromXml(a)
    case a @ <rename>{ _* }</rename> => Rename.fromXml(a)
    case a @ <remove>{ _* }</remove> => Remove.fromXml(a)
    case a @ <retype>{ _* }</retype> => Retype.fromXml(a)
  }
}

/**
 * Modifies, deletes, retains columns in the data rows.
 *
 * @author Vlad Orzhekhovskiy
 */
case class SelectValues(actions: TraversableOnce[SelectAction])
  extends GridStep1 {

  import SelectAction._

  protected def computeRDD(rdd: RDD[DataRow]): RDD[DataRow] =
    rdd map { actions.foldLeft(_)((row, action) => action(row)) }

  def outMetaData: Option[RowMetaData] = inMetaData map {
    actions.foldLeft(_)((meta, action) => action(meta))
  }

  def toXml: Elem = <select-values>{ actions map (_.toXml) }</select-values>

  def retain(names: String*) = copy(actions = actions.toSeq :+ Retain(names.toSeq))
  def rename(pairs: (String, String)*) = copy(actions = actions.toSeq :+ Rename(pairs.toMap))
  def remove(names: String*) = copy(actions = actions.toSeq :+ Remove(names.toSeq))
  def retype[T](name: String)(implicit dt: DataType[T]) = copy(actions = actions.toSeq :+ Retype(name -> dt))
  def retype(pairs: (String, DataType[_])*) = copy(actions = actions.toSeq :+ Retype(pairs.toMap))
}

/**
 * Select Values companion object.
 */
object SelectValues extends XmlFactory[SelectValues] {
  def apply(actions: SelectAction*): SelectValues = apply(actions.toSeq)
  def fromXml(xml: Node): SelectValues = apply(xml.child map SelectAction.fromXml)
}