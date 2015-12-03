package com.ignition

import scala.collection.JavaConverters.propertiesAsScalaMapConverter
import org.apache.spark.sql.{ ColumnName, Row }

/**
 * Data types, implicits, aliases for DataFrame-based workflows.
 *
 * @author Vlad Orzhekhovskiy
 */
package object frame {
  
  /**
   * An implicit conversion of:
   * $"..." literals into {{org.apache.spark.sql.ColumnName}} instances.
   * v"..." literals into VarLiteral instances.
   * e"..." literals into EnvLiteral instances.
   */
  implicit class StringToLiteral(val sc: StringContext) extends AnyVal {
    def $(args: Any*): ColumnName = new ColumnName(sc.s(args: _*))
    def v(args: Any*): VarLiteral = VarLiteral(sc.parts.head)
    def e(args: Any*): EnvLiteral = EnvLiteral(sc.parts.head)
  }
  
  /**
   * Injects row fields, environment settings and variables into the string.
   */
  def injectAll(row: Row, indexMap: Map[String, Int])(expr: String)(implicit runtime: SparkRuntime) =
    (injectGlobals _ andThen injectFields(row, indexMap))(expr)

  /**
   * Injects the fields from the specified row, by replacing substrings of form `\${field}`
   * with the value of the specified field.
   */
  def injectFields(row: Row, indexMap: Map[String, Int])(expr: String) = indexMap.foldLeft(expr) {
    case (result, (name, index)) => result.replace("${" + name + "}", row.getString(index))
  }

  /**
   * Inject JVM environment variables and spark variables by substituting e{env} and v{var} patterns
   * in the expression.
   */
  def injectGlobals(expr: String)(implicit runtime: SparkRuntime) =
    (injectEnvironment _ andThen injectVariables)(expr)

  /**
   * Injects the variables, by replacing substrings of form v{varName} with the value of
   * the specified variable.
   */
  private def injectVariables(expr: String)(implicit runtime: SparkRuntime) = runtime.vars.names.foldLeft(expr) {
    case (result, varName) => result.replace("v{" + varName + "}", runtime.vars(varName).toString)
  }

  /**
   * Injects the JVM variables by replacing the substrings of form e{varName} with the
   * value of the corresponding JVM variable.
   */
  private def injectEnvironment(expr: String) = System.getProperties().asScala.foldLeft(expr) {
    case (result, (key, value)) => result.replace("e{" + key + "}", value)
  }
}