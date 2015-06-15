package com.ignition.frame

import java.io.PrintWriter

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
case class TextFileOutput(filename: String, formats: Iterable[FieldFormat],
  separator: String = ",", outputHeader: Boolean = true) extends FrameTransformer {

  def separator(sep: String) = copy(separator = sep)
  def header(out: Boolean) = copy(outputHeader = out)

  protected def compute(arg: DataFrame, limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = {
    val filename = (injectEnvironment _ andThen injectVariables)(this.filename)
    val out = new PrintWriter(filename)

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
}

/**
 * CSV output companion object.
 */
object TextFileOutput {
  def apply(filename: String, formats: (String, String)*): TextFileOutput =
    apply(filename, formats.map(f => FieldFormat(f._1, f._2)))
}