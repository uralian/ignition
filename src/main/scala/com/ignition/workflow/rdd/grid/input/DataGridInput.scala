package com.ignition.workflow.rdd.grid.input

import scala.xml.{ Elem, Node }

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.ignition.data.{ DefaultRowMetaData, RowMetaData }
import com.ignition.data.DataRow
import com.ignition.data.DataType.StringDataType
import com.ignition.workflow.rdd.grid.{ GridStep0, XmlFactory }

/**
 * Static data grid input.
 *
 * @author Vlad Orzhekhovskiy
 */
case class DataGridInput(meta: RowMetaData, rows: Seq[DataRow], numSlices: Option[Int] = None)
  extends GridStep0 {

  validate

  protected def computeRDD(implicit sc: SparkContext): RDD[DataRow] =
    sc parallelize (rows, numSlices getOrElse defaultParallelism)

  val outMetaData: Option[RowMetaData] = Some(meta)

  def toXml: Elem =
    <grid-input>
      { meta.toXml }
      <rows>
        { rows map rowToXml }
      </rows>
    </grid-input>

  private def validate() = rows foreach { row =>
    assert(row.columnNames == meta.columnNames, "Grid meta data and column names do not match")
  }

  protected def rowToXml(row: DataRow) =
    <row>{ row.rawData map (item => <item>{ StringDataType.convert(item) }</item>) }</row>

  def addRow(row: DataRow): DataGridInput = copy(rows = rows :+ row)

  def addRow(rawData: IndexedSeq[Any]): DataGridInput = addRow(meta.row(rawData))

  def addRow(raw: Any*): DataGridInput = addRow(raw.toIndexedSeq)
}

/**
 * Static data grid companion object.
 */
object DataGridInput extends XmlFactory[DataGridInput] {

  /**
   * Creates a new DataGridInput instance.
   */
  def apply(meta: RowMetaData): DataGridInput = apply(meta, Nil)

  /**
   * Sample input:
   *
   * <grid-input>
   * 	<meta>...</meta>
   *   	<rows>
   *    	<row>
   *     		<item>d2aa8254-343f-46e2-8a64-7bffad2478de</item>
   *       		<item>ABC</item>
   *         	<item>52</item>
   *     	</row>
   *    	<row>
   *     		<item>a3fa87b1-ad63-11e4-9d3b-0a0027000000</item>
   *       		<item>XYZ</item>
   *         	<item>14</item>
   *     	</row>
   *    </rows>
   * </grid-input>
   */
  def fromXml(xml: Node) = {
    val numSlices = (xml \ "@slices").headOption map (_.text.toInt)
    val meta = DefaultRowMetaData.fromXml((xml \ "meta").head)
    val rows = (xml \\ "rows" \ "row") map { node =>
      val items = (node \ "item").zipWithIndex map {
        case (node, index) =>
          val text = if (node.text == "") null else node.text
          meta.columns(index).dataType.convert(text)
      }
      meta.row(items.toIndexedSeq)
    }
    DataGridInput(meta, rows, numSlices)
  }
}