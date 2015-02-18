package com.ignition.data

import scala.xml.{ Elem, Node }

/**
 * Encapsulates information about one column.
 *
 * @author Vlad Orzhekhovskiy
 */
final case class ColumnInfo[T](name: String)(implicit dt: DataType[T]) {
  val dataType = dt

  override def toString = s"ColumnInfo($name, ${dataType.code})"

  override def equals(other: Any) = if (other.isInstanceOf[ColumnInfo[_]]) {
    val that = other.asInstanceOf[ColumnInfo[_]]
    this.name == that.name && this.dataType == that.dataType
  } else false

  override def hashCode = 41 * name.hashCode + dataType.hashCode
}

/**
 * Provides information about a data row. Optionally provides a set of indices constituing the key.
 *
 * @author Vlad Orzhekhovskiy
 */
trait RowMetaData {

  /**
   * List of column descriptors.
   */
  def columns: IndexedSeq[ColumnInfo[_]]

  /**
   * Retrieves the index of the column by its name.
   */
  def columnIndex(name: String): Int

  /**
   * Converts meta data to XML.
   */
  def toXml: Elem

  /**
   * Returns the number of columns.
   */
  def columnCount: Int = columns.size

  /**
   * Returns the list of column names.
   */
  def columnNames = columns map (_.name)

  /**
   * Creates a new data row using this metadata's column names and supplied data.
   */
  def row(rawData: IndexedSeq[Any]): DefaultDataRow = DefaultDataRow(columnNames, rawData)

  /**
   * Creates a new data row using this metadata's column names and supplied data.
   */
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

  /**
   * Adds a column to the metadata, returning a new instance.
   */
  def ~[T](ci: ColumnInfo[T]): DefaultRowMetaData = copy(columns = columns :+ ci)

  /**
   * Adds a column to the metadata, returning a new instance.
   */
  def add[T](name: String)(implicit dt: DataType[T]): DefaultRowMetaData = this.~(ColumnInfo[T](name))
}

/**
 * RowMetaData companion object.
 */
object DefaultRowMetaData {

  /**
   * Creates a new instance of row metadata.
   */
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