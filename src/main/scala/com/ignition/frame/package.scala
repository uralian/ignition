package com.ignition

import scala.collection.JavaConverters._
import org.apache.spark.sql.Row

/**
 * Data types, implicits, aliases for DataFrame-based workflows.
 *
 * @author Vlad Orzhekhovskiy
 */
package object frame {

  /**
   * An implicit conversion of:
   * $"..." literals into FieldLiteral instances,
   * v"..." into VarLiteral instances,
   * e"..." into EnvLiteral instances.
   */
  implicit class StringToLiteral(val sc: StringContext) {
    def $(args: Any*): FieldLiteral = FieldLiteral(sc.parts.head)
    def v(args: Any*): VarLiteral = VarLiteral(sc.parts.head)
    def e(args: Any*): EnvLiteral = EnvLiteral(sc.parts.head)
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
   * Injects the variables, by replacing substrings of form %{varName} with the value of
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