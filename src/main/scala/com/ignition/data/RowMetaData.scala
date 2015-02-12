package com.ignition.data

import scala.reflect.runtime.universe._
import scala.xml._
import org.joda.time.DateTime
import com.eaio.uuid.UUID
import com.ignition.data.DataType._

/**
 * Encapsulates information about one column.
 *
 * @author Vlad Orzhekhovskiy
 */
case class ColumnInfo[T: TypeTag](name: String)(implicit dt: DataType[T]) {
  val dataType = dt
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
}

/**
 * The default implementation of RowMetaData backed by the indexed sequence of ColumnInfo instances.
 *
 * @author Vlad Orzhekhovskiy
 */
case class DefaultRowMetaData(columns: IndexedSeq[ColumnInfo[_]]) extends RowMetaData {

  require(columns.map(_.name.toLowerCase).toSet.size == columns.size, "Duplicate column name!")

  private val indexByName: Map[String, Int] = columns.map(_.name).zipWithIndex.toMap

  def columnIndex(name: String): Int = indexByName.get(name) getOrElse -1

  def toXml: Elem =
    <meta>
      { columns map (ci => <col name={ ci.name } type={ ci.dataType.code }/>) }
    </meta>
}

/**
 * RowMetaData companion object.
 */
object DefaultRowMetaData {
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
      val cType = (node \ "@type").text.toLowerCase match {
        case BooleanDataType.code => ColumnInfo[Boolean] _
        case StringDataType.code => ColumnInfo[String] _
        case IntDataType.code => ColumnInfo[Int] _
        case DoubleDataType.code => ColumnInfo[Double] _
        case DecimalDataType.code => ColumnInfo[Decimal] _
        case DateTimeDataType.code => ColumnInfo[DateTime] _
        case UUIDDataType.code => ColumnInfo[UUID] _
        case BinaryDataType.code => ColumnInfo[Binary] _
      }
      cType(name)
    }
    apply(columns: _*)
  }
}