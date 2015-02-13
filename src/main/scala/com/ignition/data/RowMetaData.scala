package com.ignition.data

import scala.xml.{ Elem, Node }

/**
 * Encapsulates information about one column.
 *
 * @author Vlad Orzhekhovskiy
 */
case class ColumnInfo[T](name: String)(implicit dt: DataType[T]) {
  val dataType = dt
  override def toString = s"ColumnInfo($name, ${dataType.code})"
}

/**
 * Provides information about a data row. Optionally provides a set of indices constituing the key.
 *
 * @author Vlad Orzhekhovskiy
 */
trait RowMetaData {
  def columns: IndexedSeq[ColumnInfo[_]]
  def columnIndex(name: String): Int
  def toXml: Elem

  def columnCount: Int = columns.size

  def columnNames = columns map (_.name)

  def row(rawData: IndexedSeq[Any]): DefaultDataRow = DefaultDataRow(columnNames, rawData)

  def row(raw: Any*): DefaultDataRow = row(raw.toIndexedSeq)
}

/**
 * The default implementation of RowMetaData backed by the indexed sequence of ColumnInfo instances.
 *
 * @author Vlad Orzhekhovskiy
 */
case class DefaultRowMetaData(columns: IndexedSeq[ColumnInfo[_]]) extends RowMetaData {

  require(columnNames.map(_.toLowerCase).toSet.size == columns.size, "Duplicate column name!")

  private val indexByName: Map[String, Int] = columnNames.zipWithIndex.toMap

  def columnIndex(name: String): Int = indexByName.get(name) getOrElse -1

  def toXml: Elem =
    <meta>
      { columns map (ci => <col name={ ci.name } type={ ci.dataType.code }/>) }
    </meta>

  def add[T](name: String)(implicit dt: DataType[T]) = copy(columns = columns :+ ColumnInfo(name))
}

/**
 * RowMetaData companion object.
 */
object DefaultRowMetaData {
  def create: DefaultRowMetaData = apply()

  def apply(columns: ColumnInfo[_]*): DefaultRowMetaData = this.apply(columns.toVector)

  /**
   * Sample input:
   *
   * <meta>
   * 	<col name="id" type="UUID" />
   *    <col name="label" type="String" />
   * 	<col name="index" type="Int" />
   * </meta>
   */
  def fromXml(xml: Node): DefaultRowMetaData = {
    val columns = (xml \\ "col") map { node =>
      val name = (node \ "@name").text
      val dataType = DataType.withCode((node \ "@type").text)
      ColumnInfo(name)(dataType)
    }
    apply(columns: _*)
  }
}