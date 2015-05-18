package com.ignition.flow

import java.io.{File, PrintWriter}

import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import com.ignition.SparkRuntime

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

  def separator(sep: String) = copy(separator = sep)
  def header(out: Boolean) = copy(outputHeader = out)

  protected def compute(arg: DataFrame, limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
    val out = new PrintWriter(file)

    if (outputHeader) {
      val header = formats map (_.name) mkString separator
      out.println(header)
    }

    val columns = formats map (ff => arg.col(ff.name)) toSeq

    val fmts = formats map (_.format) zipWithIndex

    val df = optLimit(arg, limit)
    df.select(columns: _*).collect foreach { row =>
      val line = fmts map {
        case (fmt, index) => fmt.format(row(index))
      } mkString separator
      out.println(line)
    }

    out.close

    df
  }

  protected def computeSchema(inSchema: StructType)(implicit runtime: SparkRuntime): StructType = inSchema

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