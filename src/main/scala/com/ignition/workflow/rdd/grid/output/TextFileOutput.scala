package com.ignition.workflow.rdd.grid.output

import java.io.{ File, PrintWriter }

import scala.xml.{ Elem, Node, PCData }

import org.apache.spark.rdd.RDD

import com.ignition.data.{ DataRow, RowMetaData }
import com.ignition.util.XmlUtils.{ RichNodeSeq, booleanToText }
import com.ignition.workflow.rdd.grid.{ GridStep1, XmlFactory }

/**
 * Specifies the field output format.
 */
case class FieldFormat(name: String, format: String = "%0")

/**
 * Writes rows to a CSV file.
 *
 * @author Vlad Orzhekhovskiy
 */
case class TextFileOutput(file: File, formats: Iterable[FieldFormat],
  separator: String = ",", outputHeader: Boolean = true) extends GridStep1 {

  protected def computeRDD(rdd: RDD[DataRow]): RDD[DataRow] = {
    val out = new PrintWriter(file)

    if (outputHeader) {
      val header = formats map (_.name) mkString separator
      out.println(header)
    }

    rdd.collect foreach { row =>
      val line = formats map { ff =>
        val value = row.getRaw(ff.name)
        ff.format.format(value)
      } mkString separator
      out.println(line)
    }

    out.close

    rdd
  }

  val outMetaData: Option[RowMetaData] = inMetaData

  def toXml: Elem =
    <textfile-output header={ outputHeader }>
      <file>{ PCData(file.getPath) }</file>
      <separator>{ PCData(separator) }</separator>
      <fields>
        {
          formats map { ff => <field name={ ff.name }>{ ff.format }</field> }
        }
      </fields>
    </textfile-output>

  def withoutHeader = copy(outputHeader = false)
  def withHeader = copy(outputHeader = true)
  def withSeparator(separator: String) = copy(separator = separator)

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}

/**
 * CSV Output companion object.
 */
object TextFileOutput extends XmlFactory[TextFileOutput] {

  def apply(file: File, formats: (String, String)*): TextFileOutput =
    apply(file, formats.map(f => FieldFormat(f._1, f._2)))

  def apply(filename: String, formats: (String, String)*): TextFileOutput =
    apply(new File(filename), formats.map(f => FieldFormat(f._1, f._2)))

  def fromXml(xml: Node): TextFileOutput = {
    val filename = (xml \ "file").asString
    val outputHeader = (xml \ "@header").asBoolean
    val separator = (xml \ "separator").asString
    val formats = (xml \ "fields" \ "field") map { node =>
      val name = (node \ "@name").asString
      val format = node.asString
      FieldFormat(name, format)
    }
    TextFileOutput(new File(filename), formats, separator, outputHeader)
  }
}