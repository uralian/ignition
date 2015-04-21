package com.ignition.flow

import java.io.File
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.io.PrintWriter

/**
 * Specifies the field output format.
 */
case class FieldFormat(name: String, format: String = "%s")

/**
 * Writes rows to a CSV file.
 *
 * @author Vlad Orzhekhovskiy
 */
case class TextFileOutput(file: File, formats: Iterable[FieldFormat],
  separator: String = ",", outputHeader: Boolean = true) extends Transformer {

  protected def compute(arg: DataFrame)(implicit ctx: SQLContext): DataFrame = {
    val out = new PrintWriter(file)

    if (outputHeader) {
      val header = formats map (_.name) mkString separator
      out.println(header)
    }

    val columns = formats map (ff => arg.col(ff.name)) toSeq

    val fmts = formats map (_.format) zipWithIndex

    arg.select(columns: _*).collect foreach { row =>
      val line = fmts map {
        case (fmt, index) => fmt.format(row(index))
      } mkString separator
      out.println(line)
    }

    out.close

    arg
  }

  protected def computeSchema(inSchema: Option[StructType])(implicit ctx: SQLContext): Option[StructType] =
    inSchema

  private def writeObject(out: java.io.ObjectOutputStream): Unit = unserializable
}

/**
 * CSV output companion object.
 */
object TextFileOutput {

  def apply(file: File, formats: (String, String)*): TextFileOutput =
    apply(file, formats.map(f => FieldFormat(f._1, f._2)))

  def apply(filename: String, formats: (String, String)*): TextFileOutput =
    apply(new File(filename), formats.map(f => FieldFormat(f._1, f._2)))
}