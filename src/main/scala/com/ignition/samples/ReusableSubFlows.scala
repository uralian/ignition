package com.ignition.samples

import com.ignition.SparkPlug
import com.ignition._
import com.ignition.types._
import com.ignition.frame._
import com.ignition.script._
import org.apache.spark.sql.types.Decimal

object ReusableSubFlows extends App {

  // subflow1 | simple multiplier: one input - one output
  val abSchema = int("a") ~ int("b")
  val multiplier = SubFlow(1, abSchema) { (input, output) =>
    val formula = Formula("total" -> "a * b".mvel)
    input --> formula --> output
  }

  // subflow2 | joiner/splitter: two inputs - three outputs
  val customerSchema = string("name") ~ boolean("local") ~ double("cost")
  val orderSchema = date("order_date") ~ decimal("amount") ~ string("name")
  val joiner = SubFlow(3, customerSchema, orderSchema) { (input, output) =>
    val query = SQLQuery("""
          SELECT o.order_date, o.amount, c.name, c.cost, o.amount + c.cost AS total
          FROM input0 c
          JOIN input1 o
          ON o.name = c.name
          ORDER BY c.name, total""")
    input.out(0) --> (output.in(1), query.in(0))
    input.out(1) --> (output.in(0), query.in(1))
    query --> 2 :| output
  }

  // main data flow | uses all subflows defined above
  val flow = DataFlow {
    val abGrid = DataGrid(abSchema) rows ((2, 3), (4, 2), (2, 2), (3, 1))
    val abDebug = DebugOutput()
    abGrid --> multiplier --> abDebug

    val customerGrid = DataGrid(customerSchema) rows (
      ("john", true, 25.36), ("jack", false, 74.15), ("jane", true, 19.99))
    val orderGrid = DataGrid(orderSchema) rows (
      (javaDate(2010, 1, 3), Decimal(120.55), "john"),
      (javaDate(2010, 3, 10), Decimal(42.85), "jack"),
      (javaDate(2010, 2, 9), Decimal(44.17), "john"),
      (javaDate(2010, 5, 10), Decimal(66.99), "jane"),
      (javaDate(2010, 1, 3), Decimal(55.08), "john"))
    val jDebug0 = DebugOutput()
    val jDebug1 = DebugOutput()
    val jDebug2 = DebugOutput()
    (customerGrid, orderGrid) --> joiner --> (jDebug0, jDebug1, jDebug2)

    (abDebug, jDebug0, jDebug1, jDebug2)
  }

  SparkPlug.runDataFlow(flow)

  private def javaDate(year: Int, month: Int, day: Int) = java.sql.Date.valueOf(s"$year-$month-$day")
}