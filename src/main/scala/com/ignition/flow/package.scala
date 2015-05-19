package com.ignition

import scala.collection.JavaConverters._
import org.apache.spark.sql.Row
import com.ignition.types._

/**
 * Data types, implicits, aliases for DataFrame-based workflows.
 *
 * @author Vlad Orzhekhovskiy
 */
package object flow {

  /**
   * An extension for Int to be used for connecting an output port of an MultiOutput
   * step with |: notation like this:
   * a|:2 --> 1:|b  // connect 2nd output of a to 1st input of b
   * a|:1 --> b     // connect 1st output of a to the only input of b
   */
  implicit class RichInt(val outIndex: Int) extends AnyVal {
    def -->(inIndex: Int) = OutInIndices(outIndex, inIndex)
    def -->(tgtStep: Step with SingleInput) = SInStepOutIndex(outIndex, tgtStep)
  }

  /**
   * An extension of Scala product (a1, a2, ...) object to be used for connecting
   * to the input ports of an MultiInput step.
   *
   * (a, b, c) --> d  // connect the outputs of a, b, and c to the inputs of d
   */
  implicit class RichProduct(val product: Product) extends AnyVal {
    def to(tgtStep: Step with MultiInput): tgtStep.type = {
      product.productIterator.zipWithIndex foreach {
        case (srcStep: Step with SingleOutput, index) => tgtStep.from(index, srcStep)
      }
      tgtStep
    }
    def -->(tgtStep: Step with MultiInput): tgtStep.type = to(tgtStep)
  }

  /**
   * Injects row fields, environment settings and variables into the string.
   */
  def injectAll(row: Row, indexMap: Map[String, Int])(expr: String)(implicit runtime: SparkRuntime) =
    (injectEnvironment _ andThen injectVariables andThen injectFields(row, indexMap))(expr)

  /**
   * Injects the fields from the specified row, by replacing substrings of form ${field}
   * with the value of the specified field.
   */
  def injectFields(row: Row, indexMap: Map[String, Int])(expr: String) = indexMap.foldLeft(expr) {
    case (result, (name, index)) => result.replace("${" + name + "}", row.getString(index))
  }

  /**
   * Injects the variables, by replacing substrings of form ${varName} with the value of
   * the specified variable.
   */
  def injectVariables(expr: String)(implicit runtime: SparkRuntime) = runtime.vars.names.foldLeft(expr) {
    case (result, varName) => result.replace("%{" + varName + "}", runtime.vars(varName).toString)
  }

  /**
   * Injects the JVM variables by replacing the substrings of form #{varName} with the
   * value of the corresponding JVM variable.
   */
  def injectEnvironment(expr: String) = System.getProperties().asScala.foldLeft(expr) {
    case (result, (key, value)) => result.replace("#{" + key + "}", value)
  }
}