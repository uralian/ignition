package com.ignition.workflow.rdd.grid

import scala.xml.Elem

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.ignition.data.{ DefaultDataRow, DefaultRowMetaData, RowMetaData }
import com.ignition.data.DataRow
import com.ignition.data.DataType.StringDataType
import com.ignition.workflow.Step0

/**
 * Static data grid input.
 *
 * @author Vlad Orzhekhovskiy
 */
case class DataGridInput(meta: RowMetaData, rows: Seq[DataRow], numSlices: Option[Int] = None)
  extends Step0[RDD[DataRow], SparkContext] with MetaDataHolder {

  validate

  protected def compute(sc: SparkContext): RDD[DataRow] = {
    implicit val sparkContext = sc
    sc parallelize (rows, numSlices getOrElse defaultParallelism)
  }

  val outMetaData: Option[RowMetaData] = Some(meta)

  def toXml: Elem =
    <grid>
      { meta.toXml }
      <rows>
        { rows map rowToXml }
      </rows>
    </grid>

  private def validate() = {
    val mdColumnNames = meta.columns.map(_.name)
    rows foreach { row =>
      assert(row.columnNames == mdColumnNames, "Grid meta data and column names do not match")
    }
  }

  protected def rowToXml(row: DataRow) =
    <row>{ row.rawData map (item => <item>{ StringDataType.convert(item) }</item>) }</row>

  protected def defaultPartitions(implicit sc: SparkContext) = sc.defaultMinPartitions

  protected def defaultParallelism(implicit sc: SparkContext) = sc.defaultParallelism
}

/**
 */
object DataGridInput {
  /**
   * Sample input:
   *
   * <grid>
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
   * </grid>
   */
  def fromXml(xml: Elem) = {
    val numSlices = (xml \ "@slices").headOption map (_.text.toInt)
    val meta = DefaultRowMetaData.fromXml((xml \ "meta").head)
    val columnNames = meta.columns map (_.name)
    val rows = (xml \\ "rows" \ "row") map { node =>
      val items = (node \ "item").zipWithIndex map {
        case (node, index) =>
          val text = if (node.text == "") null else node.text
          meta.columns(index).dataType.convert(text)
      }
      DefaultDataRow(columnNames, items.toVector)
    }
    DataGridInput(meta, rows, numSlices)
  }
}